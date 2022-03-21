FROM phpswoole/swoole:php8.1-alpine

RUN apk add autoconf build-base

RUN set -ex \
    && pecl update-channels \
    && pecl install redis-stable \
    && docker-php-ext-enable redis