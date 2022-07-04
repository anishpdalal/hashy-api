FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

RUN apt update && \
    apt install -y bash \
                   build-essential \
                   git \
                   curl \
                   ca-certificates \
                   g++ \
                   make \
                   cmake \
                   unzip \
                   libcurl4-openssl-dev \
                   python3 \
                   python3-pip && \
    rm -rf /var/lib/apt/lists

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

RUN mkdir -p /function

COPY ./requirements.txt /function/requirements.txt
RUN python3 -m pip install pip --upgrade
RUN python3 -m pip install --no-cache-dir --upgrade -r /function/requirements.txt

RUN mkdir -p /mnt/bi_encoder
RUN python3 -c "from sentence_transformers import SentenceTransformer; bi_encoder = SentenceTransformer('msmarco-distilbert-base-v4'); bi_encoder.save('/mnt/bi_encoder');"
COPY app.py /function
WORKDIR /function
ENTRYPOINT [ "python3", "-m", "awslambdaric" ]
CMD [ "app.handler" ]