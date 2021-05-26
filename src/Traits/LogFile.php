<?php


namespace Hyperf\Mongodb\Traits;


trait LogFile
{

    protected function addLog($path, $msg, $context = [])
    {
        if ($this->checkFileDir($path)) {
            $msg = sprintf("[%s] %s", date("Y-m-d H:i:s"), $msg);
            $context && $msg .= " | " . json_encode($context);
            $msg .= PHP_EOL;
            file_put_contents($path, $msg, FILE_APPEND);
        }
    }

    protected function checkFileDir($path)
    {
        if (rtrim($path, '/') != $path) {
            return false;
        }
        $dirs = explode('/', $path);
        $fileName = end($dirs);
        $dir = str_replace($fileName, '', $path);
        if (!is_dir($dir)) {
            return mkdir($dir, 0777, true);
        } else {
            return true;
        }
    }


}