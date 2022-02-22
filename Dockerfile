#FROM python:3.8 as builder
FROM ubuntu:18.04
ENV PYTHONIOENCODING=utf-8
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
# RUN apt-get install -y tzdata && cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
ARG DEBIAN_FRONTEND=noninteractive

ARG ssh_prv_key
ARG ssh_pub_key
ARG keyring_pass

RUN \
apt-get update -y && \
apt-get install -y apt-utils 2>&1 | \
                   grep -v "debconf: delaying package configuration, since apt-utils is not installed" && \
apt-get -qq update && \
apt-get -q -y upgrade && \
apt-get install -y sudo curl \
                        wget \
                        locales \
                        gunicorn3 \
                        openssh-server \
                        openssh-client \
                        libmysqlclient-dev \
                        sshfs && \
rm -rf /var/lib/apt/lists/*

# Ensure that we always use UTF-8 and with Canadian English locale
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
RUN apt-get update -y && apt-get install -y python3-pip python3-dev build-essential
# RUN ln -s /usr/bin/pip3 /usr/bin/pip && ln -s /usr/bin/python3.8 /usr/bin/python

# Authorize SSH Host
#RUN mkdir -p root/.ssh && \
#    chmod 0700 root/.ssh && \
#    ssh-keyscan -H 65.108.60.87 >> root/.ssh/known_hosts
#
## Add the keys and set permissions
#RUN echo "$ssh_prv_key" > /root/.ssh/id_rsa && \
#    echo "$ssh_pub_key" > /root/.ssh/id_rsa.pub && \
#    chmod 600 /root/.ssh/id_rsa && \
#    chmod 600 /root/.ssh/id_rsa.pub

#COPY ./.ssh /app
COPY ./IN_STREAM /app
COPY ./src /app
COPY . /app
WORKDIR /app

RUN pip3 install --no-cache-dir -r requirements.txt

## KEYRING
## Disable promnt `Please enter password for encrypted keyring`
#RUN sudo -E echo "$keyring_pass" | python3 -m keyring set "REDIS" "ektovav"
#RUN printf "$keyring_pass" | python3 -c "import keyring; keyring.set_password(\"REDIS\", \"ektovav\", \"$keyring_pass\")"

#RUN mkdir -p  $HOME/.local/share/python_keyring/ && \
#    echo -e "[backend]\ndefault-keyring=keyrings.alt.file.PlaintextKeyring" >> $HOME/.local/share/python_keyring/keyringrc.cfg


## Authorize SSH Host
RUN mkdir -p /ssh/
RUN chmod 0700 /ssh

RUN echo "PermitRootLogin yes" >> /etc/ssh/sshd_config
#RUN systemctl restart sshd

# Add the keys and set permissions
RUN echo "$ssh_prv_key" > /ssh/id_rsa && \
    echo "$ssh_pub_key" > /ssh/id_rsa.pub && \
    chmod 600 /ssh/id_rsa && chmod 600 /ssh/id_rsa.pub

# add bitbucket to known hosts
RUN ssh-keyscan -H 65.108.60.87 >> /ssh/known_hosts

# Copy SSH key to temp folder to pull new code
# ADD ~/.ssh/id_rsa /tmp/
# RUN ssh-agent /tmp
RUN ls -la /ssh

# check if ssh agent is running or not, if not, run
RUN eval `ssh-agent -s` && ssh-add /ssh/id_rsa

#RUN echo "user_allow_other" >> /etc/fuse.conf
#RUN mkdir -p /app/SSHSTREAM_DIR && \
#    sudo sshfs -p 22 -o allow_other -o nonempty root@65.108.60.87:/mnt/DATADIR_SSH /app/SSHSTREAM_DIR

#ADD service.py /
#ENTRYPOINT [ "python3" ]
#CMD  ["python3", "./service.py"]

EXPOSE 8003
CMD ["gunicorn3",\
     "--bind", "0.0.0.0:8003",\
     "-w", "4",\
     "--threads", "4",\
     "-k", "uvicorn.workers.UvicornWorker",\
     "service:app"]