cs1660_final_project

Docker hub repository: https://hub.docker.com/repository/docker/nid45/cs1660-final

commands (Mac or windows) - with XQuartz or Xming running//

on mac: for Exception in thread "main" java.awt.AWTError: Can't connect to X11 window server using 'host.docker.internal:0' as the value of the DISPLAY variable. run: sudo xhost +

docker pull nid45/cs1660-final

docker run -e DISPLAY=host.docker.internal:0 -v /tmp/.X11-unix:/tmp/.X11-unix nid45/cs1660-final

Demo and code walkthrough: https://www.youtube.com/watch?v=JUcei6jR624&feature=youtu.be

Running on different cluster: place credentials.json in main directory (cs1660-finalproject)

change gcpvars file so that each of the values match with the cluster you are trying to run

place Inverted.jar, TermSearch.jar and TopN.jar to a folder in cluster named JAR
