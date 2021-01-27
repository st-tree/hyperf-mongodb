<?php

return [
    'default' => [
        'mode' => 0,
        'settings' => [
            0 => [
                'host' => env('MONGODB_HOST', '127.0.0.1'),
                'port' => env('MONGODB_PORT', 27017),
                'username' => env('MONGODB_USERNAME', ''),
                'password' => env('MONGODB_PASSWORD', ''),
                'db' => env('MONGODB_DB', 'test'),
                //设置复制集,没有不设置
                //'replica' => 'rs0',
                //设置读偏好,没有不设置
                //'readPreference' => 'secondaryPreferred'
            ],
            1 => [
                'host' => [
                    env('MONGODB_HOST_1', '127.0.0.1'),
                    env('MONGODB_HOST_2', '127.0.0.1'),
                ],
                'port' => [
                    env('MONGODB_PORT_1', 27017),
                    env('MONGODB_PORT_2', 27017),
                ],
                'username' => env('MONGODB_USERNAME', ''),
                'password' => env('MONGODB_PASSWORD', ''),
                'db' => env('MONGODB_DB', 'test'),
                //设置复制集,没有不设置
                //'replica' => 'rs0',
                //设置读偏好,没有不设置
                //'readPreference' => 'secondaryPreferred'
            ],
        ],
        'authMechanism' => 'SCRAM-SHA-256',
        'pool' => [
            'min_connections' => 3,
            'max_connections' => 1000,
            'connect_timeout' => 10.0,
            'wait_timeout' => 3.0,
            'heartbeat' => -1,
            'max_idle_time' => (float) env('MONGODB_MAX_IDLE_TIME', 60),
        ],
    ],
];