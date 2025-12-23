FROM phpswoole/swoole:php8.3-alpine

RUN apk add autoconf build-base

RUN docker-php-ext-enable redis

ADD https://github.com/mlocati/docker-php-extension-installer/releases/latest/download/install-php-extensions /usr/local/bin/

RUN docker-php-ext-configure pcntl --enable-pcntl

RUN docker-php-ext-install pcntl
