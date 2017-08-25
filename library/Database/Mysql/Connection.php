<?php
/**
 * Created by PhpStorm.
 * User: liuwang-s
 * Date: 2015/11/5
 * Time: 15:42
 */
namespace Database\Mysql;
class Connection{

    protected static $pdo = array();

    /*
     * @desc need to set which host to connect
     * @param $param string;flag of which db
     * @return obj;pdo instance
     * */
    protected Static function getInstance($param){
        /*if has been pdo instance, just return*/
         if (isset(self::$pdo[$param])
             && self::$pdo[$param] instanceof \PDO) {
             return self::$pdo[$param];
         }

        /*fetch config of db from yaf config.ini*/
        $config = \Yaf\Registry::get('config')->mysql->$param;

        /*make pdo connect*/
        $dsn = "mysql:host=" . $config['host'] . ";port=" . $config['port'] . ";dbname=" . $config['dbname'];

        try {
            self::$pdo[$param] = new \PDO($dsn, $config['username'], $config['password'], array(
                \PDO::MYSQL_ATTR_LOCAL_INFILE => true
            ));
            self::$pdo[$param]->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
            self::$pdo[$param]->query("use {$config['dbname']};");
            self::$pdo[$param]->query("set names utf8");

            return self::$pdo[$param];
        } catch ( \PDOException $e ) {
            return false;
        }
    }
}