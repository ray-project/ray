from unittest import mock
import sys
from datetime import datetime, timezone
import pytest

from ci.ray_ci.automation.docker_tags_lib import (
    _get_docker_auth_token,
    _get_docker_hub_auth_token,
    _get_image_creation_time,
    backup_release_tags,
    copy_tag_to_aws_ecr,
    delete_tag,
    _list_recent_commit_short_shas,
    query_tags_from_docker_hub,
    query_tags_from_docker_with_oci,
    _is_release_tag,
    list_image_tags,
    get_ray_commit,
    check_image_ray_commit,
    AuthTokenException,
    RetrieveImageConfigException,
    DockerHubRateLimitException,
)


@mock.patch("requests.get")
def test_get_docker_auth_token(mock_requests):
    namespace = "test_namespace"
    repository = "test_repository"
    mock_requests.return_value = mock.Mock(
        status_code=200, json=mock.MagicMock(return_value={"token": "test_token"})
    )

    token = _get_docker_auth_token(namespace, repository)

    assert mock_requests.call_count == 1
    assert (
        mock_requests.call_args[0][0]
        == f"https://auth.docker.io/token?service=registry.docker.io&scope=repository:{namespace}/{repository}:pull"  # noqa
    )
    assert token == "test_token"


@mock.patch("requests.get")
def test_get_docker_auth_token_no_token(mock_requests):
    namespace = "test_namespace"
    repository = "test_repository"
    mock_requests.return_value = mock.Mock(
        status_code=200, json=mock.MagicMock(return_value={})
    )

    token = _get_docker_auth_token(namespace, repository)

    assert mock_requests.call_count == 1
    assert (
        mock_requests.call_args[0][0]
        == f"https://auth.docker.io/token?service=registry.docker.io&scope=repository:{namespace}/{repository}:pull"  # noqa
    )
    assert token is None


@mock.patch("requests.get")
def test_get_docker_auth_token_failure(mock_requests):
    namespace = "test_namespace"
    repository = "test_repository"
    mock_requests.return_value = mock.Mock(
        status_code=400, json=mock.MagicMock(return_value={"error": "test_error"})
    )

    with pytest.raises(AuthTokenException, match="Docker. Error code: 400"):
        _get_docker_auth_token(namespace, repository)


@mock.patch("requests.post")
def test_get_docker_hub_auth_token(mock_requests):
    mock_requests.return_value = mock.Mock(
        status_code=200, json=mock.MagicMock(return_value={"token": "test_token"})
    )

    token = _get_docker_hub_auth_token(
        username="test_username", password="test_password"
    )

    assert mock_requests.call_count == 1
    assert mock_requests.call_args[0][0] == "https://hub.docker.com/v2/users/login"
    assert mock_requests.call_args[1]["headers"] == {"Content-Type": "application/json"}
    assert mock_requests.call_args[1]["json"] == {
        "username": "test_username",
        "password": "test_password",
    }
    assert token == "test_token"


@mock.patch("requests.post")
def test_get_docker_hub_auth_token_failure(mock_requests):
    mock_requests.return_value = mock.Mock(
        status_code=400, json=mock.MagicMock(return_value={"error": "test_error"})
    )

    with pytest.raises(AuthTokenException, match="Docker Hub. Error code: 400"):
        _get_docker_hub_auth_token(username="test_username", password="test_password")


@mock.patch("ci.ray_ci.automation.docker_tags_lib._get_git_log")
def test_list_recent_commit_short_shas(mock_git_log):
    mock_git_log.return_value = "a1b2c3\nq2w3e4\nt5y678"

    shas = _list_recent_commit_short_shas()

    assert mock_git_log.call_count == 1
    assert shas == ["a1b2c3", "q2w3e4", "t5y678"]


@mock.patch("ci.ray_ci.automation.docker_tags_lib._get_git_log")
def test_list_recent_commit_short_shas_empty(mock_git_log):
    mock_git_log.return_value = ""

    shas = _list_recent_commit_short_shas(n_days=30)

    mock_git_log.assert_called_once_with(n_days=30)
    assert shas == []


@mock.patch("ci.ray_ci.automation.docker_tags_lib._get_config_docker_oci")
def test_get_image_creation_time(mock_config_docker_oci):
    mock_config_docker_oci.return_value = {
        "architecture": "amd64",
        "created": "2024-01-26T23:00:00.000000000Z",
        "variant": "v1",
    }
    created_time = _get_image_creation_time(tag="test_namespace/test_repo:test_tag")

    mock_config_docker_oci.assert_called_once_with(
        tag="test_tag", namespace="test_namespace", repository="test_repo"
    )
    assert created_time == datetime(2024, 1, 26, 23, 0, tzinfo=timezone.utc)


@mock.patch("ci.ray_ci.automation.docker_tags_lib._get_config_docker_oci")
def test_get_image_creation_time_failure_unknown(mock_config_docker_oci):
    mock_config_docker_oci.side_effect = RetrieveImageConfigException(
        "Failed to retrieve image config."
    )

    with pytest.raises(RetrieveImageConfigException):
        _get_image_creation_time(tag="test_namespace/test_repo:test_tag")
    mock_config_docker_oci.assert_called_once_with(
        tag="test_tag", namespace="test_namespace", repository="test_repo"
    )


@mock.patch("ci.ray_ci.automation.docker_tags_lib._get_config_docker_oci")
def test_get_image_creation_time_failure_no_create_time(mock_config_docker_oci):
    mock_config_docker_oci.return_value = {"architecture": "amd64", "variant": "v1"}
    with pytest.raises(RetrieveImageConfigException):
        _get_image_creation_time(tag="test_namespace/test_repo:test_tag")
    mock_config_docker_oci.assert_called_once_with(
        tag="test_tag", namespace="test_namespace", repository="test_repo"
    )


@mock.patch("requests.delete")
def test_delete_tag(mock_requests):
    namespace = "test_namespace"
    repository = "test_repository"
    tag = "test_tag"
    mock_requests.return_value = mock.Mock(status_code=204)

    deleted = delete_tag(
        tag=f"{namespace}/{repository}:{tag}", docker_hub_token="test_token"
    )
    mock_requests.assert_called_once()
    assert (
        mock_requests.call_args[0][0]
        == f"https://hub.docker.com/v2/repositories/{namespace}/{repository}/tags/{tag}"
    )
    assert mock_requests.call_args[1]["headers"] == {
        "Authorization": "Bearer test_token"
    }
    assert deleted is True


@mock.patch("requests.delete")
def test_delete_tag_failure(mock_requests):
    namespace = "test_namespace"
    repository = "test_repository"
    tag = "test_tag"
    mock_requests.return_value = mock.Mock(status_code=400)

    deleted = delete_tag(
        tag=f"{namespace}/{repository}:{tag}", docker_hub_token="test_token"
    )
    mock_requests.assert_called_once()
    assert (
        mock_requests.call_args[0][0]
        == f"https://hub.docker.com/v2/repositories/{namespace}/{repository}/tags/{tag}"
    )
    assert mock_requests.call_args[1]["headers"] == {
        "Authorization": "Bearer test_token"
    }
    assert deleted is False


@mock.patch("requests.delete")
def test_delete_tag_failure_rate_limit_exceeded(mock_requests):
    namespace = "test_namespace"
    repository = "test_repository"
    tag = "test_tag"
    mock_requests.return_value = mock.Mock(status_code=429)

    with pytest.raises(DockerHubRateLimitException):
        delete_tag(tag=f"{namespace}/{repository}:{tag}", docker_hub_token="test_token")
    mock_requests.assert_called_once()
    assert (
        mock_requests.call_args[0][0]
        == f"https://hub.docker.com/v2/repositories/{namespace}/{repository}/tags/{tag}"
    )
    assert mock_requests.call_args[1]["headers"] == {
        "Authorization": "Bearer test_token"
    }


@pytest.mark.parametrize(
    ("tag", "release_versions", "expected_value"),
    [
        ("2.0.0", ["2.0.0"], True),
        ("2.0.0rc0", ["2.0.0rc0"], True),
        ("2.0.0-py38", ["2.0.0"], True),
        ("2.0.0-py38-cu123", ["2.0.0"], True),
        ("2.0.0.post1", ["2.0.0.post1"], True),
        ("2.0.0.1", ["2.0.0"], False),
        ("2.0.0.1r", ["2.0.0"], False),
        ("a.1.c", ["2.0.0"], False),
        ("1.a.b", ["2.0.0"], False),
        ("2.0.0rc0", ["2.0.0"], False),
        ("2.0.0", ["2.0.0rc0"], False),
        ("2.0.0.a1s2d3", ["2.0.0"], False),
        ("2.0.0.a1s2d3-py38-cu123", ["2.0.0"], False),
        ("2.0.0", None, True),
    ],
)
def test_is_release_tag(tag, release_versions, expected_value):
    assert _is_release_tag(tag, release_versions) == expected_value


@mock.patch("ci.ray_ci.automation.docker_tags_lib._call_crane_cp")
def test_copy_tag_to_aws_ecr(mock_call_crane_cp):
    tag = "test_namespace/test_repository:test_tag"
    mock_call_crane_cp.return_value = (
        0,
        "aws-ecr/name/repo:test_tag: digest: sha256:sample-sha256 size: 1788",
    )

    is_copied = copy_tag_to_aws_ecr(tag, "aws-ecr/name/repo")
    mock_call_crane_cp.assert_called_once_with(
        tag="test_tag", source=tag, aws_ecr_repo="aws-ecr/name/repo"
    )
    assert is_copied is True


@mock.patch("ci.ray_ci.automation.docker_tags_lib._call_crane_cp")
def test_copy_tag_to_aws_ecr_failure(mock_call_crane_cp):
    tag = "test_namespace/test_repository:test_tag"
    mock_call_crane_cp.return_value = (1, "Error: Failed to copy tag.")

    is_copied = copy_tag_to_aws_ecr(tag, "aws-ecr/name/repo")
    mock_call_crane_cp.assert_called_once_with(
        tag="test_tag", source=tag, aws_ecr_repo="aws-ecr/name/repo"
    )
    assert is_copied is False


def _make_docker_hub_response(
    tag: str, page_count: int, namespace: str, repository: str, page_limit: int
):
    next_url = f"https://hub.docker.com/v2/namespaces/{namespace}/repositories/{repository}/tags?page={page_count+1}&page_size=1"  # noqa
    return mock.Mock(
        status_code=200,
        json=mock.MagicMock(
            return_value={
                "count": page_limit,
                "next": next_url if page_count < page_limit else None,
                "previous": "previous_url",
                "results": [{"name": tag}],
            }
        ),
    )


# TODO: use actual filter functions for real use case once they are merged
@pytest.mark.parametrize(
    ("filter_func", "tags", "expected_tags"),
    [
        (
            lambda t: t.startswith("a1s2d3"),
            ["a1s2d3-py38", "2.7.0.q2w3e4", "t5y678-py39-cu123"],
            ["a1s2d3-py38"],
        ),
        (
            lambda t: t.startswith("2.7.0"),
            ["a1s2d3-py38", "2.7.0.q2w3e4", "t5y678-py39-cu123"],
            ["2.7.0.q2w3e4"],
        ),
    ],
)
@mock.patch("requests.get")
def test_query_tags_from_docker_hub(mock_requests, filter_func, tags, expected_tags):
    mock_requests.side_effect = [
        _make_docker_hub_response(
            tags[i], i + 1, "test_namespace", "test_repo", len(tags)
        )
        for i in range(len(tags))
    ]
    queried_tags = query_tags_from_docker_hub(
        filter_func=filter_func,
        namespace="test_namespace",
        repository="test_repo",
        docker_hub_token="test_token",
    )
    expected_tags = sorted([f"test_namespace/test_repo:{t}" for t in expected_tags])

    assert mock_requests.call_count == len(tags)
    assert queried_tags == expected_tags


# TODO: use actual filter functions for real use case once they are merged
@pytest.mark.parametrize(
    ("filter_func", "tags", "expected_tags"),
    [
        (
            lambda t: t.startswith("a1s2d3"),
            ["a1s2d3-py38", "2.7.0.q2w3e4", "t5y678-py39-cu123"],
            ["a1s2d3-py38"],
        ),
    ],
)
@mock.patch("requests.get")
def test_query_tags_from_docker_hub_failure(
    mock_requests, filter_func, tags, expected_tags
):
    mock_requests.side_effect = [
        _make_docker_hub_response(tags[0], 1, "test_namespace", "test_repo", len(tags)),
        mock.Mock(
            status_code=403,
            json=mock.MagicMock(
                return_value={"detail": "Forbidden", "message": "Forbidden"}
            ),
        ),
    ]
    queried_tags = query_tags_from_docker_hub(
        filter_func=filter_func,
        namespace="test_namespace",
        repository="test_repo",
        docker_hub_token="test_token",
    )
    expected_tags = sorted([f"test_namespace/test_repo:{t}" for t in expected_tags[:1]])

    assert mock_requests.call_count == 2
    assert queried_tags == expected_tags


@mock.patch("ci.ray_ci.automation.docker_tags_lib._get_docker_auth_token")
@mock.patch("requests.get")
def test_query_tags_from_docker_with_oci(mock_requests, mock_get_token):
    mock_get_token.return_value = "test_token"
    mock_requests.return_value = mock.Mock(
        status_code=200,
        json=mock.MagicMock(
            return_value={
                "tags": ["test_tag1", "test_tag2", "test_tag3"],
            }
        ),
    )

    tags = query_tags_from_docker_with_oci("test_namespace", "test_repo")
    assert tags == [
        "test_namespace/test_repo:test_tag1",
        "test_namespace/test_repo:test_tag2",
        "test_namespace/test_repo:test_tag3",
    ]


@mock.patch("ci.ray_ci.automation.docker_tags_lib._get_docker_auth_token")
@mock.patch("requests.get")
def test_query_tags_from_docker_with_oci_failure(mock_requests, mock_get_token):
    mock_get_token.return_value = "test_token"
    mock_requests.return_value = mock.Mock(
        status_code=401,
        json=mock.MagicMock(
            return_value={
                "Error": "Unauthorized",
            }
        ),
    )

    with pytest.raises(Exception, match="Failed to query tags"):
        query_tags_from_docker_with_oci("test_namespace", "test_repo")


@mock.patch("ci.ray_ci.automation.docker_tags_lib._get_docker_hub_auth_token")
@mock.patch("ci.ray_ci.automation.docker_tags_lib.query_tags_from_docker_hub")
@mock.patch("ci.ray_ci.automation.docker_tags_lib._write_to_file")
@mock.patch("ci.ray_ci.automation.docker_tags_lib.copy_tag_to_aws_ecr")
def test_backup_release_tags(
    mock_copy_tag, mock_write, mock_query_tags, mock_get_token
):
    namespace = "test_namespace"
    repository = "test_repository"
    aws_ecr_repo = "test_aws_ecr_repo"

    mock_get_token.return_value = "test_token"
    mock_query_tags.return_value = [
        f"{namespace}/{repository}:2.0.{i}" for i in range(10)
    ]

    backup_release_tags(
        namespace=namespace,
        repository=repository,
        aws_ecr_repo=aws_ecr_repo,
        docker_username="test_username",
        docker_password="test_password",
    )

    assert mock_query_tags.call_count == 1
    assert mock_query_tags.call_args.kwargs["namespace"] == namespace
    assert mock_query_tags.call_args.kwargs["repository"] == repository
    assert mock_query_tags.call_args.kwargs["docker_hub_token"] == "test_token"
    assert mock_write.call_count == 1
    assert mock_copy_tag.call_count == 10
    for i, call_arg in enumerate(mock_copy_tag.call_args_list):
        assert call_arg.kwargs["aws_ecr_repo"] == aws_ecr_repo
        assert call_arg.kwargs["tag"] == f"{namespace}/{repository}:2.0.{i}"


@pytest.mark.parametrize(
    (
        "prefix",
        "ray_type",
        "python_versions",
        "platforms",
        "architectures",
        "expected_tags",
    ),
    [
        (
            "test",
            "ray",
            ["3.9"],
            ["cpu", "cu11.8.0"],
            ["x86_64", "aarch64"],
            [
                "test",
                "test-aarch64",
                "test-cpu",
                "test-cpu-aarch64",
                "test-cu118",
                "test-cu118-aarch64",
                "test-gpu",
                "test-gpu-aarch64",
                "test-py39",
                "test-py39-aarch64",
                "test-py39-cpu",
                "test-py39-cpu-aarch64",
                "test-py39-cu118",
                "test-py39-cu118-aarch64",
                "test-py39-gpu",
                "test-py39-gpu-aarch64",
            ],
        ),
        (
            "test",
            "ray-ml",
            ["3.9"],
            ["cpu", "cu11.8.0"],
            ["x86_64"],
            [
                "test",
                "test-cpu",
                "test-cu118",
                "test-gpu",
                "test-py39",
                "test-py39-cpu",
                "test-py39-cu118",
                "test-py39-gpu",
            ],
        ),
    ],
)
def test_list_image_tags(
    prefix, ray_type, python_versions, platforms, architectures, expected_tags
):
    tags = list_image_tags(prefix, ray_type, python_versions, platforms, architectures)
    assert tags == sorted(expected_tags)


@pytest.mark.parametrize(
    ("prefix", "ray_type", "python_versions", "platforms", "architectures"),
    [
        (
            "test",
            "ray",
            ["3.8"],
            ["cpu", "cu11.8.0"],
            ["x86_64", "aarch64"],
        ),  # python version not supported
        (
            "test",
            "ray",
            ["3.9"],
            ["cpu", "cu14.0.0"],
            ["x86_64"],
        ),  # platform not supported
        (
            "test",
            "ray",
            ["3.9"],
            ["cpu", "cu11.8.0"],
            ["aarch32"],
        ),  # architecture not supported
        (
            "test",
            "ray-ml",
            ["3.9"],
            ["cpu", "cu11.8.0"],
            ["aarch64"],
        ),  # architecture not supported
        (
            "test",
            "ray-ml",
            ["3.9"],
            ["cpu", "cu11.5.2"],
            ["x86_64"],
        ),  # platform not supported
        (
            "test",
            "not-ray",
            ["3.8"],
            ["cpu", "cu11.8.0"],
            ["x86_64"],
        ),  # ray type not supported
    ],
)
def test_list_images_tags_failure(
    prefix, ray_type, python_versions, platforms, architectures
):
    with pytest.raises(ValueError):
        list_image_tags(prefix, ray_type, python_versions, platforms, architectures)


@mock.patch("docker.from_env")
def test_get_ray_commit(mock_docker_from_env):
    expected_commit = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"
    # Setup mock
    mock_client = mock_docker_from_env.return_value
    mock_container = mock_client.containers.run.return_value
    mock_container.decode.return_value = expected_commit

    commit = get_ray_commit("test_repo/test_image:tag")

    assert mock_client.containers.run.call_count == 1
    assert (
        mock_client.containers.run.call_args.kwargs["image"]
        == "test_repo/test_image:tag"
    )
    assert commit == expected_commit


@mock.patch("docker.from_env")
def test_get_ray_commit_failure(mock_docker_from_env):
    expected_commit = "a1b2c3d4e5f6a7b8c9d0e"
    # Setup mock
    mock_client = mock_docker_from_env.return_value
    mock_container = mock_client.containers.run.return_value
    mock_container.decode.return_value = expected_commit

    with pytest.raises(Exception):
        get_ray_commit("test_repo/test_image:tag")


@mock.patch("ci.ray_ci.automation.docker_tags_lib.get_ray_commit")
@mock.patch("ci.ray_ci.automation.docker_tags_lib.list_image_tags")
@mock.patch("ci.ray_ci.automation.docker_tags_lib.pull_image")
@mock.patch("ci.ray_ci.automation.docker_tags_lib.remove_image")
def test_check_image_ray_commit(
    mock_remove_image, mock_pull_image, mock_list_tags, mock_get_commit
):
    expected_commit = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"
    ray_type = "ray"
    mock_get_commit.return_value = expected_commit
    mock_list_tags.return_value = ["test-tag1", "test-tag2", "test-tag3", "test-tag4"]

    check_image_ray_commit(
        prefix="test", ray_type=ray_type, expected_commit=expected_commit
    )

    assert mock_pull_image.call_count == len(mock_list_tags.return_value)
    assert mock_pull_image.call_args_list == [
        mock.call(f"rayproject/{ray_type}:{tag}") for tag in mock_list_tags.return_value
    ]

    assert mock_get_commit.call_count == len(mock_list_tags.return_value)
    assert mock_get_commit.call_args_list == [
        mock.call(f"rayproject/{ray_type}:{tag}") for tag in mock_list_tags.return_value
    ]

    # Remove all images except the first one
    assert mock_remove_image.call_count == len(mock_list_tags.return_value) - 1
    assert mock_remove_image.call_args_list == [
        mock.call(f"rayproject/{ray_type}:{tag}")
        for tag in mock_list_tags.return_value[1:]
    ]


@mock.patch("ci.ray_ci.automation.docker_tags_lib.get_ray_commit")
@mock.patch("ci.ray_ci.automation.docker_tags_lib.list_image_tags")
@mock.patch("ci.ray_ci.automation.docker_tags_lib.pull_image")
@mock.patch("ci.ray_ci.automation.docker_tags_lib.remove_image")
def test_check_image_ray_commit_failure(
    mock_remove_image, mock_pull_image, mock_list_tags, mock_get_commit
):
    expected_commit = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"
    ray_type = "ray"
    mock_get_commit.return_value = expected_commit
    mock_list_tags.return_value = ["test-tag1", "test-tag2", "test-tag3", "test-tag4"]

    with pytest.raises(SystemExit):
        check_image_ray_commit(
            prefix="test", ray_type=ray_type, expected_commit=expected_commit[::-1]
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
