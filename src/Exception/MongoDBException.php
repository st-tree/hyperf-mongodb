<?php

namespace Hyperf\Mongodb\Exception;

class MongoDBException extends \RuntimeException
{
    /**
     * @param string $msg
     * @throws MongoDBException
     */
    public static function managerError(string $msg)
    {
        throw new self($msg);
    }
}