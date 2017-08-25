<?php
/**
 * Created by PhpStorm.
 * User: liuwang-s
 * Date: 2015/11/9
 * Time: 17:18
 */
namespace Database\Mysql;
class MysqlQuery {
    private $queryBuilder;
    private $timeStampFlag;

    public function __construct(){
        $this->queryBuilder = new QueryBuilder();
    }

    /*
     * @desc set pdo param(which host to connect)
     * */
    protected function setMysqlParam($str){
        $this->queryBuilder->mysqlParam = $str;

        return true;
    }

    /*
     * @desc set table name
     * */
    protected function setTableName($name){
        $subClassName = get_called_class();
        if(!empty($subClassName)){
            $arrTableMap = \Yaf\Registry::get('tableMap');
            if(isset($arrTableMap[$subClassName]) && !empty($arrTableMap[$subClassName])){
                $name = $arrTableMap[$subClassName];
            }
        }
        $this->queryBuilder->tableName = $name;

        return true;
    }

    /*
     * @desc set alias name of table, attention,
     *       its lifetime only continues in one query
     * @param $aliasName; alias to set
     * */
    public function setTableNameAlias($aliasName){
        $this->queryBuilder->tableNameAlias = $aliasName;

        return true;
    }

    /*
     * @desc active the created_time and updated_time selfUpdate
     * */
    public function setTimeStamp(){
        $this->timeStampFlag = true;
    }

    /*
     * @desc start transaction
     * */
    public function beginTransaction(){
        $this->queryBuilder->pdoInit();
        if($this->queryBuilder->pdoClient->inTransaction()){
            $this->commit();
        }

        $this->queryBuilder->pdoClient->beginTransaction();

        return true;
    }

    /*
     * @desc commit a transaction
     * */
    public function commit(){
        $this->queryBuilder->pdoClient->commit();

        return true;
    }

    /*
     * @desc roll back a query
     * */
    public function rollBack(){
        $this->queryBuilder->pdoClient->rollBack();

        return true;
    }

    public function quote($param){
        $this->queryBuilder->pdoInit();
        return $this->queryBuilder->pdoClient->quote($param);
    }

    /*
     * @desc insert(trigger execute)
     * */
    public function insert(array $arrInput){
        if($this->timeStampFlag === true){
            $arrInput = array_merge($arrInput, array(
                'created_time' => date('Y-m-d H:i:s'),
                'updated_time' => date('Y-m-d H:i:s'),
            ));
        }
        $this->queryBuilder->buildInsert($arrInput);
        return $this->queryBuilder->execute();
    }

    /*
     * @desc on duplicate update
     * */
    public function insertOnDup(array $arrInsert, array $arrUpdate){
        if($this->timeStampFlag === true){
            $arrInsert = array_merge($arrInsert, array(
                'created_time' => date('Y-m-d H:i:s'),
                'updated_time' => date('Y-m-d H:i:s'),
            ));
            $arrUpdate = array_merge($arrUpdate, array(
                'updated_time' => date('Y-m-d H:i:s'),
            ));
        }

        $this->queryBuilder->buildInsert($arrInsert);
        $this->queryBuilder->buildDupUpdate($arrUpdate);

        return $this->queryBuilder->execute();
    }

    /*
     * @desc delete(trigger execute)
     * */
    public function delete(){
        $this->queryBuilder->buildDelete();
        return $this->queryBuilder->execute();
    }

    /*
     * @desc update(trigger execute)
     * */
    public function update(array $arrInput){
        if($this->timeStampFlag === true){
            $arrInput = array_merge($arrInput, array(
                'updated_time' => date('Y-m-d H:i:s'),
            ));
        }
        $this->queryBuilder->buildUpdate($arrInput);
        return $this->queryBuilder->execute();
    }

    /*
     * @desc select(trigger execute)
     * @params mixed(string/array)
     * */
    public function select($input = ''){
        $this->queryBuilder->buildSelect($input);
        return $this->queryBuilder->execute();
    }

    /*
     * @desc findOne(trigger execute)
     * @params mixed(string/array)
     * */
    public function findOne($input = ''){
        $this->limit(1);
        $this->queryBuilder->buildSelect($input);
        $arrRes = $this->queryBuilder->execute();
        if(false === $arrRes){
            return false;
        }

        return isset($arrRes[0]) ? $arrRes[0] : array();
    }

    /*
     * @desc set select content
     * @param $input mixed(string/array)
     * @return object of self
     * */
    public function subSelect($input = ''){
        $this->queryBuilder->buildSelect($input);
        return $this;
    }

    /*
     * @desc set sub table alias
     * @param $aliasName; tmp table alias name
     * @return array('table' => '', 'params' => array())
     * */
    public function subAlias($aliasName){
        $arrSqlParam = $this->queryBuilder->execute(true);
        if(isset($arrSqlParam['table'])
            && !empty($arrSqlParam['table'])){
            $arrSqlParam['table'] = '(' . $arrSqlParam['table'] . ') as ' . $aliasName;
        }

        return $arrSqlParam;
    }

    /*
     * @desc append the params to current pdo object params
     * @param $params array;append to current query params
     * */
    public function appendParams(array $params){
        $this->queryBuilder->params = array_merge($this->queryBuilder->params, $params);

        return $this;
    }

    /*
     * @desc join
     * */
    public function join($tableName){
        $this->queryBuilder->buildJoin($tableName);
        return $this;
    }

    /*
     * @desc join condition on
     * @param str input string. (table1.name1=table2.name2)
     * */
    public function on($str){
        $this->queryBuilder->buildOn($str);
        return $this;
    }

    /*
     * @desc where condition filter
     * @param 2 or 3, if 2, $operator is =
     * */
    public function where($column, $operator, $val = ''){
        if(func_num_args() == 2){
            $val = $operator;
            $operator = '=';
        }
        $this->queryBuilder->buildWhere($column, $operator, $val);
        return $this;
    }

    /*
     * @desc whereRaw
     * @param $str string;filter condition
     * */
    public function whereRaw($str){
        if(!empty($str)){
            $this->queryBuilder->buildWhereRaw($str);
        }

        return $this;
    }

    /*
     * @desc whereIn
     * */
    public function whereIn($column, array $arr){
        $this->queryBuilder->buildWhereIn($column, $arr);
        return $this;
    }

    /*
     * @desc whereIn
     * */
    public function whereNotIn($column, array $arr){
        if(empty($arr)){
            return $this;
        }
        $this->queryBuilder->buildWhereIn($column, $arr, true);
        return $this;
    }

    /*
     * @desc limit skip number set
     * */
    public function skip($num){
        $this->queryBuilder->buildSkip((int)$num);
        return $this;
    }

    /*
     * @desc group by function
     * @param $str string
     * */
    public function groupBy($str){
        $this->queryBuilder->buildGroupBy($str);
        return $this;
    }

    /*
     * @desc orderBy
     * @param $column order by who
     * @param $orderType in:asc,desc
     * */
    public function orderBy($column, $orderType = 'asc'){
        $this->queryBuilder->buildOrdeBy($column, $orderType);
        return $this;
    }

    /*
     * @desc set limit number
     * */
    public function limit($num){
        $this->queryBuilder->buildLimit((int)$num);
        return $this;
    }

    /*
     * @desc count number of items
     * */
    public function count(){
        $this->queryBuilder->buildCount();
        $arrCount = $this->queryBuilder->execute();
        if(is_array($arrCount) && isset($arrCount[0])){
            foreach ($arrCount[0] as $count) {
                return (int)$count;
            }
        }
        return false;
    }

    /*
     * @desc get all queries of this request
     * */
    public function getQueryLog(){
        return $this->queryBuilder->arrQueryLog;
    }

    /*
     * @desc get the last query of this request
     * */
    public function getLastQueryLog(){
        return $this->queryBuilder->arrQueryLog[count($this->queryBuilder->arrQueryLog) - 1];
    }

    /*
     * @tool function
     * @param $arr two-dimensional array
     * @param $key value of whick column to extract
     * @return one-dimensional array
     * */
    public function toList($arr, $column){
        if(!is_array($arr)){
            return array();
        }
        if(phpversion() > '5.5.0'){
            return array_column($arr, $column);
        }
        //make old php version right
        $arrRet = array();
        foreach ($arr as $val) {
            if(isset($val[$column])){
                array_push($arrRet, $val[$column]);
            }
        }
        return $arrRet;
    }

    public function arrayToMap($arrInput, $key1, $key2 = ''){
        if(!is_array($arrInput)){
            return $arrInput;
        }
        $arrRet = array();
        if(empty($key1)){
            return $arrRet;
        }
        if(empty($key2)){
            foreach($arrInput as $arr){
                if(!isset($arr[$key1])){
                    continue;
                }
                $arrRet[$arr[$key1]] = $arr;
            }
            return $arrRet;
        }
        foreach($arrInput as $arr){
            if(!isset($arr[$key1]) || !isset($arr[$key2])){
                continue;
            }
            $arrRet[$arr[$key1]] = $arr[$key2];
        }
        return $arrRet;
    }
}