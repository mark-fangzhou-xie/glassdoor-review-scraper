FROM jupyter/scipy-notebook:6c3390a9292e

USER root

RUN apt-get -y update && \
	apt-get -y install unzip xvfb libxi6 libgconf-2-4 default-jdk curl gnupg


RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
	echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list && \
	apt-get update -y && \
	apt-get install -y google-chrome-stable
	

ENV CHROMEDRIVER_VERSION=81.0.4044.138
ENV CHROMEDRIVER_DIR=/chromedriver
RUN mkdir -p $CHROMEDRIVER_DIR

# Download and install Chromedriver
RUN wget -q --continue -P $CHROMEDRIVER_DIR "http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip"
RUN unzip $CHROMEDRIVER_DIR/chromedriver* -d $CHROMEDRIVER_DIR

RUN chmod +x $CHROMEDRIVER_DIR/chromedriver

# Put Chromedriver into the PATH
ENV PATH $CHROMEDRIVER_DIR:$PATH

COPY requirements.txt /tmp/
COPY . /home/jovyan/work/

RUN chown $NB_UID work
RUN pip install --requirement /tmp/requirements.txt && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

USER $NB_UID
