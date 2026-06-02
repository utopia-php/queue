# PHPStan Stubs

This directory contains stub files for PHPStan static analysis.

## RedisCluster.stub.php

This file is the official stub file from the [phpredis](https://github.com/phpredis/phpredis) extension.

- **Source**: https://github.com/phpredis/phpredis/blob/develop/redis_cluster.stub.php
- **Purpose**: Provides type definitions for Redis Streams methods (xAdd, xGroup, xReadGroup, etc.) and other RedisCluster methods
- **License**: PHP License (same as phpredis extension)

The phpredis extension provides these stub files in their repository for static analysis tools like PHPStan. Since the stub files are not distributed with the compiled extension, we include them here for PHPStan to use during static analysis.

This is the recommended approach from the phpredis maintainers for using their extension with static analysis tools.
