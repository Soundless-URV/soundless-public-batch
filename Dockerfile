FROM python:3.10.5-slim-buster AS compile-image

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY setup.py .
COPY batch_pubsub_to_cos .
RUN pip install .

FROM python:3.10.5-slim-buster AS build-image
COPY --from=compile-image /opt/venv /opt/venv

# Install dependency needed by rtree (Python package for spatial queries in geopandas)
RUN apt-get update
RUN apt-get install -y libspatialindex-dev
RUN mkdir /home/data

ARG town_borders_folder=town_borders
ARG regions_yaml=regions.yaml

ENV PATH="/opt/venv/bin:$PATH"
ENV PROJECT_ID="XXXXXXXX"
ENV TOPIC_ID="XXXXXXXX"
ENV SUBSCRIPTION_ID="XXXXXXX"
ENV BUCKET_NAME="soundless-data"
ENV OUTPUT_FOLDER="/home/data"
ENV TOWN_BORDERS_PATH="/home/${town_borders_folder}/bm5mv21sh0tpm1_20200601_0.shp"
ENV REGIONS_YAML_PATH="/home/${regions_yaml}"

WORKDIR /home
COPY ${town_borders_folder}/ ${town_borders_folder}/
COPY ${regions_yaml} ${regions_yaml}

CMD ["batch_pubsub_to_cos"]
