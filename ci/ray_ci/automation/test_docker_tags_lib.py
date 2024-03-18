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
    list_docker_image_versions,
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
    ("prefix", "ray_type", "expected_tags"),
    [
        (
            "nightly",
            "ray",
            [
                "nightly",
                "nightly-cpu",
                "nightly-cpu-aarch64",
                "nightly-cu115",
                "nightly-cu115-aarch64",
                "nightly-cu116",
                "nightly-cu116-aarch64",
                "nightly-cu117",
                "nightly-cu117-aarch64",
                "nightly-cu118",
                "nightly-cu118-aarch64",
                "nightly-cu121",
                "nightly-cu121-aarch64",
                "nightly-gpu",
                "nightly-gpu-aarch64",
                "nightly-aarch64",
                "nightly-py39",
                "nightly-py39-cpu",
                "nightly-py39-cpu-aarch64",
                "nightly-py39-cu115",
                "nightly-py39-cu115-aarch64",
                "nightly-py39-cu116",
                "nightly-py39-cu116-aarch64",
                "nightly-py39-cu117",
                "nightly-py39-cu117-aarch64",
                "nightly-py39-cu118",
                "nightly-py39-cu118-aarch64",
                "nightly-py39-cu121",
                "nightly-py39-cu121-aarch64",
                "nightly-py39-gpu",
                "nightly-py39-gpu-aarch64",
                "nightly-py39-aarch64",
                "nightly-py310",
                "nightly-py310-cpu",
                "nightly-py310-cpu-aarch64",
                "nightly-py310-cu115",
                "nightly-py310-cu115-aarch64",
                "nightly-py310-cu116",
                "nightly-py310-cu116-aarch64",
                "nightly-py310-cu117",
                "nightly-py310-cu117-aarch64",
                "nightly-py310-cu118",
                "nightly-py310-cu118-aarch64",
                "nightly-py310-cu121",
                "nightly-py310-cu121-aarch64",
                "nightly-py310-gpu",
                "nightly-py310-gpu-aarch64",
                "nightly-py310-aarch64",
                "nightly-py311-cpu",
                "nightly-py311-cpu-aarch64",
                "nightly-py311-cu115",
                "nightly-py311-cu115-aarch64",
                "nightly-py311-cu116",
                "nightly-py311-cu116-aarch64",
                "nightly-py311-cu117",
                "nightly-py311-cu117-aarch64",
                "nightly-py311-cu118",
                "nightly-py311-cu118-aarch64",
                "nightly-py311-cu121",
                "nightly-py311-cu121-aarch64",
                "nightly-py311-gpu",
                "nightly-py311-gpu-aarch64",
                "nightly-py311",
                "nightly-py311-aarch64",
            ],
        ),
        (
            "nightly",
            "ray-ml",
            [
                "nightly",
                "nightly-cpu",
                "nightly-gpu",
                "nightly-cu118",
                "nightly-py39",
                "nightly-py39-cpu",
                "nightly-py39-gpu",
                "nightly-py39-cu118",
                "nightly-py310",
                "nightly-py310-cpu",
                "nightly-py310-gpu",
                "nightly-py310-cu118",
            ]
        ),
    ],
)
def test_list_docker_image_versions(prefix, ray_type, expected_tags):
    tags = list_docker_image_versions(prefix, ray_type)
    assert len(tags) == len(expected_tags)
    expected_tags = set(expected_tags)
    for tag in tags:
        assert tag in expected_tags

def test_list_docker_image_versions_failure():
    with pytest.raises(ValueError):
        list_docker_image_versions("nightly", "not-ray")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
