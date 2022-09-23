ARG APP_HOME=/opt/broadlink/

# ============================================================================ #
# Development stage: used only to create the python virtual environment        #
# ============================================================================ #
FROM tdmproject/alpine-python-3:arm32v7-3.13 AS development

LABEL maintainer="Massimo Gaggero <massimo dot gaggero at crs4 dot it>"

ARG APP_HOME


# Install required packages
RUN apk add --no-cache git gcc python3-dev musl-dev linux-headers libffi-dev \
	py3-pip rust cargo openssl-dev && \
	pip install --no-cache-dir virtualenv && \
	mkdir -p ${APP_HOME}

WORKDIR ${APP_HOME}

COPY requirements.txt requirements.txt

RUN virtualenv venv && \
	. venv/bin/activate && \
	pip install --upgrade pip && \
	pip install --no-cache-dir -r requirements.txt

# ============================================================================ #
# Final stage: lean, intended to be used as execute container                  #
# docker build --target=final -f docker/Dockerfile \                           #
#              -t tdmproject/edge-broadlink-handler .                          #
# ============================================================================ #
FROM tdmproject/alpine-python-3:arm32v7-3.13 AS final

ARG APP_HOME

ENV APP_HOME=${APP_HOME}

COPY --from=development ${APP_HOME} ${APP_HOME}
WORKDIR ${APP_HOME}

COPY src src

ENTRYPOINT ["src/entrypoint.sh"]

CMD [ "-c", "/opt/configs/tdm.conf" ]

# ============================================================================ #
# Testing Stage: final stage for testing purposes                              #
# docker build --target=testing -f docker/Dockerfile \                         #
#              -t tdmproject/edge-broadlink-handler .                          #
# ============================================================================ #
FROM final AS testing

ARG APP_HOME

ENV APP_HOME=${APP_HOME}

WORKDIR ${APP_HOME}

COPY tests tests
