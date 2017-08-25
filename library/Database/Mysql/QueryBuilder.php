<?php
/**
 * Created by PhpStorm.
 * User: liuwang-s
 * Date: 2015/11/9
 * Time: 17:37
 */
namespace Database\Mysql;

class QueryBuilder extends Connection{
    private $actions;
    private $sql;
    public $params;           //pdo params
    private $sqlType;

    public $pdoClient;
    public $tableName;
    public $tableNameAlias;
    public $fields;
    public $mysqlParam;
    public $arrQueryLog;
    public $transactionFlag;

    const LIMIT_NUM = 1000;

    public function __construct(){
        $this->initActions();
        $this->arrQueryLog = array();
        $this->sqlType = '';
        $this->sql = '';
        $this->transactionFlag = 0;
    }

    /*
     * @desc set flag of transaction
     * */
    public function setTransactionFlag(){
        $this->transactionFlag = 1;

        return true;
    }

    /*
     * @desc unset flag of transaction
     * */
    public function unsetTransactionFlag(){
        $this->transactionFlag = 0;

        return true;
    }

    /*
     * @desc first query, init pdo
     * */
    public function pdoInit(){
        if($this->pdoClient instanceof \PDO){
            return true;
        }
        $this->pdoClient = parent::getInstance($this->mysqlParam);
    }

    /*
     * @desc init some var,also need to reinit in process
     * */
    private function initActions(){
        $this->actions = array(
            'insert' => '',
            'delete' => '',
            'update' => '',
            'select' => '',
            'join' => '',
            'on' => '',
            'where' => '',
//            'whereIn' => '',
            'whereRaw' => '',
            'groupBy' => '',
            'orderBy' => '',
            'limit' => '',
        );
        $this->params = array();
        $this->tableNameAlias = '';
    }

    /*
     * @desc get table name function
     * */
    private function getTableName(){
        return empty($this->tableNameAlias) ?
            $this->tableName : $this->tableName . ' as ' . $this->tableNameAlias;
    }

    /*
     * @desc fetch the query sql string
     * @return string
     * */
    public function getQuerySql(){
        return $this->sql;
    }

    /*
     * @desc fetch the query param
     * @return array
     * */
    public function getQueryParam(){
        return $this->params;
    }

    /*
     * @desc avoid the same key of params
     * */
    private function checkParamKey($key){
        /*avoid appear "." in the key */
        $key = str_replace('.', '_', $key, $outputNum);
        if($outputNum == 0){
            $key = $this->getTableName() . '_' .$key;
        }

        $count = 1;
        while(1) {
            if (isset($this->params[':' . $key])) {
                $key = $key . '_' . $count;
                $count++;
                continue;
            }else{
                return $key;
            }
        }
    }

    /*
     * @desc from action array to sql string
     * @return string
     * */
    private function generateSql(){
        $this->sql = '';
        foreach ($this->actions as $action) {
            if($action === ''){
                continue;
            }
            $this->sql .= $action . ' ';
        }
        $this->sql = trim($this->sql);

        return true;
    }

    /*
     * @desc query result fetch
     * */
    private function fetchQueryResult($stmt){
        switch($this->sqlType){
            case 'select':
                return $stmt->fetchAll(\PDO::FETCH_ASSOC);
            case 'insert':
                return $this->pdoClient->lastInsertId();   //lastInsertId() is PDO function
            case 'update':
                return $stmt->rowCount();
            case 'delete':
                return $stmt->rowCount();
            default:
                return false;
        }
    }

    /*
     * @desc SQL execute
     * @params string
     * */
    private function query(){
        $tmpParams = $this->params;

        //update log
        array_push($this->arrQueryLog, array(
            'sql' => $this->sql,
            'params' => $tmpParams,
        ));

        //reset actions and params
        $this->initActions();

        $stmt = $this->pdoClient->prepare($this->sql);
        try{
            if($stmt->execute($tmpParams)){
                return $this->fetchQueryResult($stmt);
            }
        }catch (\Exception $e){
            if(defined('CODE_ENV')
                && CODE_ENV == 'develop'){
				echo json_encode(array(
					'errcode' => 99999,
					'errmsg'  => $e->getMessage() . ' ######please check sql',
					'data'    => $this->arrQueryLog[count($this->arrQueryLog) - 1];
				));
				exit;
            }
            if($this->pdoClient->inTransaction()){
                throw $e;
            }
            return false;
        }
    }

    /*
     * @desc build insert
     * */
    public function buildInsert(array $arrInput){
        //set sql type
        $this->sqlType = 'insert';

        $keys = array_keys($arrInput);

        //pack insert sql string
        $strColumn = '';
        $strVal = '';
        foreach ($keys as $key) {
            $strColumn .= $key . ',';
            $strVal .= ':' . $key . ',';
        }
        $strColumn = trim($strColumn, ',');
        $strVal = trim($strVal, ',');

        $this->actions['insert'] = 'insert into ' . $this->getTableName() .
            ' (' . $strColumn . ')' . ' values(' . $strVal . ')';

        //add value to pdo params
        foreach ($arrInput as $key => $val) {
            $this->params[':'. $key] = $val;
        }

        return $this;
    }

    /*
     * @desc build on duplicate update
     * */
    public function buildDupUpdate(array $arrInput){
        $this->actions['insert'] .= ' ON DUPLICATE KEY UPDATE ';

        //add value to pdo params
        foreach ($arrInput as $key => $val) {
            $this->actions['insert'] .= $key . ' = :' . $key . ',';
            $this->params[':'. $key] = $val;
        }
        $this->actions['insert'] = trim($this->actions['insert'], ',');

        return $this;
    }

    /*
     * @desc build delete
     * */
    public function buildDelete(){
        $this->sqlType = 'delete';

        $this->actions['delete'] = 'delete from ' . $this->getTableName();

        return $this;
    }

    /*
     * @desc build update
     * */
    public function buildUpdate(array $arrInput){
        $this->sqlType = 'update';

        $this->actions['update'] = 'update ' . $this->getTableName() . ' set ';
        foreach ($arrInput as $key => $val) {
            $this->actions['update'] .= $key . ' = :' . $key . ',';
            $this->params[':' . $key] = $val;
        }
        $this->actions['update'] = trim($this->actions['update'], ',');

        return $this;
    }

    /*
     * @desc build Select
     * @params mixed(string/array)
     * */
    public function buildSelect($input = ''){
        //set sql type
        $this->sqlType = 'select';

        $str = 'select ';
        if($input === ''){
            $str .= '* ';
        }elseif(is_array($input)) {
            foreach ($input as $val) {
                $str .= $val . ',';
            }
            $str = trim($str, ',');
        }else{
            $str .= $input;
        }
        $str .= ' from ' . $this->getTableName();
        $this->actions['select'] = $str;

        return $this;
    }

    /*
     * @desc join
     * */
    public function buildJoin($tableName){
        if(empty($tableName)){
//            throw new JoinParamInValid;
        }
        $this->actions['join'] = ' join ' . $tableName;
        return $this;
    }

    /*
     * @desc join condition on
     * @param str input string. (table1.name1=table2.name2)
     * */
    public function buildOn($str){
        if(empty($str)){
//            throw new OnParamInValid;
        }
        $this->actions['on'] = ' on ' . $str;
        return $this;
    }

    /*
     * @desc where condition filter
     * @param 2 or 3, if 2, $operator is =
     * */
    public function buildWhere($column, $operator, $val){
        if(empty($this->actions['where'])){
            $this->actions['where'] .= ' where ';
        }else {
            $this->actions['where'] .= ' and ';
        }
        $paramKey = $this->checkParamKey($column);
        $this->actions['where'] .= $column . ' ' . $operator . ' :' . $paramKey;
        $this->params[':' . $paramKey] = $val;
        return $this;
    }

    /*
     * @desc whereRaw condition filter
     * @param $str filter condition description
     * */
    public function buildWhereRaw($str){
        if(empty($this->actions['where'])){
            $this->actions['where'] .= ' where (';
        }else {
            $this->actions['where'] .= ' and (';
        }
        $this->actions['where'] .= $str . ')';

        return $this;
    }

    /*
     * @desc whereIn
     * */
    public function buildWhereIn($column, array $arr, $notFlag = false){
        if(empty($column) || empty($arr)){
//            throw new MysqlException(MysqlError::WHEREIN_PARAM_INVALID);
        }

        $strTmp = '';
        foreach ($arr as $val) {
            if(is_string($val)){
                $strTmp .= '"' . $val . '",';
            }else{
                $strTmp .= $val . ',';
            }
        }
        $strTmp = trim($strTmp, ',');

        if(empty($this->actions['where'])){
            $this->actions['where'] .= ' where ';
        }else {
            $this->actions['where'] .= ' and ';
        }
        if($notFlag){
            $this->actions['where'] .= $column . ' not in (' . $strTmp . ')';
        }else {
            $this->actions['where'] .= $column . ' in (' . $strTmp . ')';
        }
        return $this;
    }

    /*
     * @desc limit skip number set
     * */
    public function buildSkip($num){
        if(empty($this->actions['limit'])){
            $this->actions['limit'] = 'limit ' . $num . ',' . self::LIMIT_NUM;
        }else{
            $arrTmp = explode(',', $this->actions['limit']);
            $this->actions['limit'] = 'limit ' . $num . ',' . $arrTmp[1];
        }

        return $this;
    }

    /*
     * @desc groupby
     * @param $str string eg. id,type
     * */
    public function buildGroupBy($str){
        $this->actions['groupBy'] = 'group by ' . $str;

        return $this;
    }

    /*
     * @desc order by function
     * */
    public function buildOrdeBy($column, $orderType = 'asc'){
        if(!in_array(strtolower($orderType), array('asc', 'desc'))){
            $orderType = 'asc';
        }
        if(empty($this->actions['orderBy'])) {
            $this->actions['orderBy'] = 'order by ' . $column . ' ' . $orderType;
        }else{
            $this->actions['orderBy'] .= ', ' . $column . ' ' . $orderType;
        }

        return $this;
    }

    /*
     * @desc set limit number
     * */
    public function buildLimit($num){
        if(empty($this->actions['limit'])) {
            $this->actions['limit'] = 'limit 0,' . $num;
        }else{
            $arrTmp = explode(',', $this->actions['limit']);
            $this->actions['limit'] = $arrTmp[0] . ',' . $num;
        }
        return $this;
    }

    /*
     * @desc count number of items
     * */
    public function buildCount(){
        $this->buildSelect('count(*) as count');
        return $this;
    }

    /*
     * @desc check pdo, generate sql and execute query
     * @param $subQuery if subQuery, do not execute
     * @return array
     * */
    public function execute($subQuery = false){
        //check or init pdo instance
        $this->pdoInit();

        //according actions[] generate sql string
        $this->generateSql();

        /*if no need execute, just return sql and params*/
        if($subQuery){
            $arrRet =  array(
                'table' => $this->getQuerySql(),
                'params' => $this->getQueryParam(),
            );
            $this->initActions();

            return $arrRet;
        }

        return $this->query();
    }
}
