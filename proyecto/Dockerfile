FROM openjdk:14-alpine

ENV SPARK_HOME=/usr/lib/python3.7/site-packages/pyspark

RUN apk add bash
RUN apk add build-base
RUN apk add nano
RUN apk add postgresql-client
RUN apk add python3
RUN apk add python3-dev
RUN apk add libffi-dev
RUN apk add libxslt-dev
RUN apk add openblas-dev
RUN apk add zlib-dev
RUN apk add jpeg-dev
RUN apk add zeromq-dev

RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install notebook
RUN pip3 install numpy
RUN pip3 install scipy
RUN pip3 install scikit-learn
RUN pip3 install matplotlib
RUN pip3 install seaborn
RUN pip3 install pytest
RUN pip3 install pytest-sugar
RUN pip3 install pyspark
RUN pip3 install findspark
RUN pip3 install gdown
RUN pip3 install jupyter_contrib_nbextensions
RUN pip3 install jupyter_nbextensions_configurator

RUN jupyter contrib nbextension install --user
RUN jupyter nbextensions_configurator enable --user

RUN jupyter nbextension enable contrib_nbextensions_help_item/main
RUN jupyter nbextension enable autosavetime/main
RUN jupyter nbextension enable codefolding/main
RUN jupyter nbextension enable code_font_size/code_font_size
RUN jupyter nbextension enable code_prettify/code_prettify
RUN jupyter nbextension enable collapsible_headings/main
RUN jupyter nbextension enable comment-uncomment/main
RUN jupyter nbextension enable equation-numbering/main
RUN jupyter nbextension enable execute_time/ExecuteTime 
RUN jupyter nbextension enable gist_it/main 
RUN jupyter nbextension enable hide_input/main 
RUN jupyter nbextension enable spellchecker/main
RUN jupyter nbextension enable toc2/main
RUN jupyter nbextension enable toggle_all_line_numbers/main

RUN ln /usr/bin/python3.7 /usr/bin/python

WORKDIR /src

COPY . /src
