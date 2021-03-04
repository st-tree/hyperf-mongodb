<?php


namespace Hyperf\Mongodb;

use Hyperf\Mongodb\Exception\MongoDBException;
use Hyperf\Mongodb\Pool\PoolFactory;
use Hyperf\Utils\Context;

/**
 * Class MongoDb
 * @package Hyperf\Mongodb
 */
class MongoDb
{
    use Attributes;
    /**
     * @var PoolFactory
     */
    protected $factory;

    /**
     * @var string
     */
    protected $poolName = 'default';

    protected $casts = [];

    protected $defaults = [];

    protected $table = '';

    public function __construct(PoolFactory $factory)
    {
        $this->factory = $factory;
    }

    public function setTable(string $table)
    {
        $this->table = $table;
        return $this;
    }

    public function setPoolName(string $poolName)
    {
        $this->poolName = $poolName;
        return $this;
    }

    public function setCasts(array $casts)
    {
        $this->casts = $casts;
        return $this;
    }

    public function setDefaults(array $defaults)
    {
        $this->defaults = $defaults;
        return $this;
    }


    /**
     * 重新封装 获取数据
     * @param $where array
     * @param array $field array 取值例子 ['_id'=>1]
     * @param array $sort array
     * @param int $skip int
     * @param int $limit int
     * @return array
     * @throws MongoDBException
     */
    public function get($where, $field = [], $sort = ['_id' => 1], $skip = 0, $limit = 0)
    {
        try {
            $option = [
                'skip' => $skip,
                'limit' => $limit,
                'projection' => $field,
                'sort' => $sort,
            ];
            $mailInfo = $this->fetchAll($where, $option);
            return $mailInfo;
        } catch (\Throwable $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }
    
    
    /**
     * 返回满足filer的全部数据
     *
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
     */
    public function fetchAll(array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            !empty($filter) && $filter = $this->relationsAttribute($filter);
            return $collection->executeQueryAll($this->table, $filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 返回满足filer的分页数据
     *
     * @param int $limit
     * @param int $currentPage
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
     */
    public function fetchPagination(int $limit, int $currentPage, array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            !empty($filter) && $filter = $this->relationsAttribute($filter);
            return $collection->execQueryPagination($this->table, $limit, $currentPage, $filter, $options);
        } catch (\Exception  $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 批量插入
     * @param array $data
     * @return bool|string
     * @throws MongoDBException
     */
    public function insertAll(array $data)
    {
        if (count($data) == count($data, 1)) {
            throw new  MongoDBException('data is can only be a two-dimensional array');
        }
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            foreach ($data as $key => &$value) {
                $value = $this->setUnsetValueByDefaults($value);
                $value = $this->relationsAttribute($value);
                $value = $this->sortAttribute($value);
            }
            return $collection->insertAll($this->table, $data);
        } catch (MongoDBException $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 数据插入数据库
     * @Task
     * @param array $data
     * @return bool|mixed
     * @throws MongoDBException
     */
    public function insert(array $data = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            $data = $this->setUnsetValueByDefaults($data);
            $data = $this->relationsAttribute($data);
            $data = $this->sortAttribute($data);
            return $collection->insert($this->table, $data);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 更新数据满足$filter的行的信息成$newObject
     *
     * @param array $filter
     * @param array $newObj
     * @return bool
     * @throws MongoDBException
     */
    public function updateRow(array $filter = [], array $newObj = []): bool
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            $filter = $this->relationsAttribute($filter);
            $newObj = $this->relationsAttribute($newObj);
            return $collection->updateRow($this->table, $filter, $newObj);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 只更新数据满足$filter的行的列信息中在$newObject中出现过的字段
     *
     * @param array $filter
     * @param array $newObj
     * @return bool
     * @throws MongoDBException
     */
    public function updateColumn(array $filter = [], array $newObj = []): bool
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            $filter = $this->relationsAttribute($filter);
            $newObj = $this->relationsAttribute($newObj);
            return $collection->updateColumn($this->table, $filter, $newObj);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 删除满足条件的数据，默认只删除匹配条件的第一条记录，如果要删除多条$limit=true
     *
     * @param array $filter
     * @param bool $limit
     * @return bool
     * @throws MongoDBException
     */
    public function delete(array $filter = [], bool $limit = false): bool
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            !empty($filter) && $filter = $this->relationsAttribute($filter);
            return $collection->delete($this->table, $filter, $limit);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 返回collection中满足条件的数量
     *
     * @param array $filter
     * @return bool
     * @throws MongoDBException
     */
    public function count(array $filter = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            !empty($filter) && $filter = $this->relationsAttribute($filter);
            return $collection->count($this->table, $filter);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }
    
    public function createIndex($indexName, $indexArr, $unique = false)
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->createIndex($this->table, $indexName, $indexArr, $unique);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }


    /**
     * 聚合查询
     * @param array $where
     * @param array $filter
     * @param array $group
     * @param array $sort
     * @return bool
     * @throws MongoDBException
     * @throws \MongoDB\Driver\Exception\Exception
     */
    public function command(array $where = [],array $filter = [],array $group = [],array $sort = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            !empty($where) && $where = $this->relationsAttribute($where);
            $pipeline = $this->getPipeline($where,$filter,$group,$sort);
            return $collection->command($this->table, $pipeline);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    private function getConnection()
    {
        $connection = null;
        $hasContextConnection = Context::has($this->getContextKey());
        if ($hasContextConnection) {
            $connection = Context::get($this->getContextKey());
        }
        if (!$connection instanceof MongoDbConnection) {
            $pool = $this->factory->getPool($this->poolName);
            $connection = $pool->get()->getConnection();
        }
        return $connection;
    }

    /**
     * The key to identify the connection object in coroutine context.
     */
    private function getContextKey(): string
    {
        return sprintf('mongodb.connection.%s', $this->poolName);
    }


    /**
     * @param array $where
     * @param int $skip
     * @param int $limit
     * @param array $sort
     * @param array $fields
     * @return array        返回pipeline
     */
    private function getPipeline(array $where = [], array $fields = [],array $group=[], array $sort = [])
    {
        $arr=[];
        !empty($where)  &&  $arr[] = ['$match'  =>$where];
        !empty($fields) &&  $arr[] = ['$project'=>$fields];
        !empty($group)  &&  $arr[] = ['$group'  =>$group];
        !empty($sort)   &&  $arr[] = ['$sort'   =>$sort];
        return $arr;
    }


    private function castAttribute(string $castType, $value)
    {
        switch ($castType) {
            case 'int':
            case 'integer':
                return $this->fromInt($value);
            case 'string':
                return $this->fromString($value);
            case 'bool':
            case 'boolean':
                return $this->fromBool($value);
            case 'object':
                return $this->fromIdObj($value);
        }
        return $value;
    }

    /**
     * @param $attributes mixed 查询参数
     * @param string $contextKey 递归时传递的上下文键名
     * @return array
     */
    private function relationsAttribute($attributes,$contextKey = '')
    {
        if (is_array($attributes)) {
            $castAttribute = [];
            foreach($attributes as $aKey => $aValue){
                if (isset($this->casts[$aKey]) || $contextKey) {
                    if ($contextKey) {
                        $type = $this->casts[$contextKey];
                    } else {
                        $type = $this->casts[$aKey];
                    }
                    if (is_array($aValue)) {
                        strpos($aKey,'$')===false && $contextKey = $aKey;
                        foreach($aValue as $k => $v){
                            $castAttribute[$aKey][$k] = $this->relationsAttribute($v,$contextKey);
                        }
                        $contextKey = '';
                    } elseif (is_string($aValue)) {
                        $castAttribute[$aKey] = $this->castAttribute($type, $aValue);
                    } else {
                        $castAttribute[$aKey] = $aValue;
                    }
                } elseif(strpos($aKey,'$')!==false) {
                    foreach($aValue as $k => $v){
                        $castAttribute[$aKey][$k] = $this->relationsAttribute($v);
                    }
                } else {
                    $castAttribute[$aKey] = $aValue;
                }
            }
        } else {
            $type = $this->casts[$contextKey];
            $castAttribute = $this->castAttribute($type, $attributes);
        }
        return $castAttribute;
    }

    public function setCasts(array $casts = [])
    {
        $this->casts = $casts;
    }

    public function setUnsetValueByDefaults(&$attributes)
    {
        foreach ($this->defaults as $key => $default) {
            if (!isset($attributes[$key])) {
                $attributes[$key] = $default;
            }
        }
        return $attributes;
    }

    private function sortAttribute($attributes)
    {
        $sortedData = [];
        foreach ($this->casts as $key => $type) {
            if (isset($attributes[$key])) {
                $sortedData[$key] = $attributes[$key];
                unset($attributes[$key]);
            }
        }
        $sortedData = array_merge($sortedData, $attributes);
        return $sortedData;
    }
}
