FROM ubuntu:latest

RUN apt update && apt install  openssh-server sudo -y

#RUN useradd -rm -d /home/ubuntu -s /bin/bash -g root -G sudo -u 1000 test 

RUN  echo 'test:test' | chpasswd

RUN mkdir -p /home/ubuntu/.ssh

COPY testkey.pub /home/ubuntu/.ssh/testkey.pub

RUN service ssh start

EXPOSE 22

CMD ["/usr/sbin/sshd","-D"]

