from ci.ray_ci.automation.docker_tags_lib import get_image_creation_time

time = get_image_creation_time("khluu/getting-started:0.0.1-khluu")
print(time)