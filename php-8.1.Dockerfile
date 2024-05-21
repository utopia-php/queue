FROM composer:2.0 as composer

WORKDIR /usr/local/src/

COPY composer.lock /usr/local/src/
COPY composer.json /usr/local/src/

RUN composer install --ignore-platform-reqs

FROM appwrite/utopia-base:php-8.1-0.1.0 as final

RUN docker-php-ext-configure pcntl --enable-pcntl

RUN docker-php-ext-install pcntl

WORKDIR /usr/local/src/

COPY . .

COPY --from=composer /usr/local/src/vendor /usr/local/src/vendor

CMD ["sleep","3600"]
