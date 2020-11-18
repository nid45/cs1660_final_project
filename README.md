cs1660_final_project

Docker hub repository:  https://hub.docker.com/repository/docker/nid45/cs1660-final

commands (Mac) - with XQuartz running//

docker pull nid45/cs1660-final
docker run -e DISPLAY=host.docker.internal:0 -v /tmp/.X11-unix:/tmp/.X11-unix nid45/cs1660-final
