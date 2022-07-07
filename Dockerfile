FROM composer:2.0 as composer

WORKDIR /usr/local/src/

COPY composer.lock /usr/local/src/
COPY composer.json /usr/local/src/

RUN composer install --ignore-platform-reqs --optimize-autoloader \
    --no-plugins --no-scripts --prefer-dist \
    `if [ "$TESTING" != "true" ]; then echo "--no-dev"; fi`

FROM phpswoole/swoole:php8.1-alpine

WORKDIR /usr/local/src/
RUN apk add autoconf build-base

RUN set -ex \
    && pecl update-channels \
    && pecl install redis-stable \
    && docker-php-ext-enable redis

COPY . .

COPY --from=composer /usr/local/src/vendor /usr/src/code/vendor

CMD ["sleep","3600"]
