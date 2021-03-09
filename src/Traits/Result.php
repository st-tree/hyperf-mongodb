<?php

namespace Hyperf\Mongodb\Traits;

use MongoDB\BSON\ObjectId;

trait Result
{
    /**
     * 返回结果格式化
     * @param $result
     * @return mixed
     */
    public function resultFormatter($result)
    {
        if ($result && is_array($result)) {
            foreach ($result as &$row) {
                if (is_array($row) || is_object($row)) {
                    foreach ($row as $key => &$value) {
                        if (!is_array($value)) {
                            //普通键值
                            $value = $this->formatter($value, $key);
                        } else {
                            //lookup的数值
                            $value = $this->resultFormatter($value);
                        }
                    }
                }

            }
            $result = $this->toArray($result);
            return $result;
        }
    }

    /**
     * 数值格式化
     * @param $value
     * @param string $key
     * @return string
     */
    public function formatter($value, $key = '')
    {
        if (
            $value instanceof ObjectId
            || ($key && isset($this->casts[$key]) && $this->casts[$key] == 'object')
        ) {
            $value = (string)$value;
        }
        return $value;
    }

    /**
     * 对象/含对象的数组 转换为 数组
     * @param $obj
     * @return mixed
     */
    public function toArray($obj)
    {
        $json = json_encode($obj);
        return json_decode($json, true);
    }

    /**
     * 提取lookup的结果值, 存到原数组
     * @param $result
     * @param array $extractCfgs  key:lookup结果在原数组中的键名  value:lookup结果中要提取的键名   (可选)new_key:lookup提取的值存放到原数组中的新键名, 默认为value的值   例子:[{"key":"crm_mail_sub","value":"cm_body_html"},{"key":"crm_mail_sub","value":"api_data","new_key":"api_data"}]
     * @return mixed
     */
    public function extractLookupValues($result, $extractCfgs = [])
    {
        foreach ($result as &$mail) {
            foreach ($extractCfgs as $conf) {
                if (isset($mail[$conf['key']])) {
                    if (isset($mail[$conf['key']][0][$conf['value']])) {
                        $v = $mail[$conf['key']][0][$conf['value']];
                        if (isset($conf['new_key']) && $conf['new_key']) {
                            $mail[$conf['new_key']] = $v;
                        } else {
                            $mail[$conf['value']] = $v;
                        }
                    }
                }
            }
            foreach ($extractCfgs as $conf) {
                if (isset($mail[$conf['key']])) {
                    unset($mail[$conf['key']]);
                }
            }
        }
        return $result;
    }
}