<?php
namespace DreamFactory\Core\Cassandra\Components;

use DreamFactory\Core\Exceptions\InternalServerErrorException;

class CassandraClient
{
    /**
     * DB session
     * @var null
     */
    protected $session = null;

    /**
     * @var null
     */
    protected $keyspace = null;

    protected $schema = null;

    public function __construct(array $config)
    {
        $hosts = array_get($config, 'hosts');
        $port = array_get($config, 'port', 9042);
        $keyspace = array_get($config, 'keyspace');
        $username = array_get($config, 'username');
        $password = array_get($config, 'password');

        if(empty($hosts)){
            throw new InternalServerErrorException('No Cassandra host(s) provided in configuration.');
        }

        $cluster = \Cassandra::cluster()
                        ->withContactPoints($hosts)
                        ->withPersistentSessions(true)
                        ->withPort($port);

        if(!empty($username) && !empty($password)){
            $cluster->withCredentials($username, $password);
        }

        $this->session = $cluster->build()->connect($keyspace);
        $this->schema = $this->session->schema();
        $this->keyspace = $this->schema->keyspace($keyspace);
    }
    
    public function getSession()
    {
        return $this->session;
    }

    public function getKeyspace()
    {
        return $this->keyspace;
    }

    public function getSchema()
    {
        return $this->schema;
    }

    public function listTables()
    {
        $tables = $this->keyspace->tables();
        $out = [];
        foreach($tables as $table){
            $out[] = ['table_name' => $table->name()];
        }

        return $out;
    }

    public function getTable($name)
    {
        return $this->keyspace->table($name);
    }

    /**
     * @param $cql
     *
     * @return \Cassandra\SimpleStatement
     */
    public function prepareStatement($cql)
    {
        return new \Cassandra\SimpleStatement($cql);
    }

    /**
     * @param       $statement
     * @param array $options
     *
     * @return mixed
     */
    public function executeStatement($statement, array $options = [])
    {
        if(!empty($options)){
            return $this->session->execute($statement, new \Cassandra\ExecutionOptions($options));
        } else {
            return $this->session->execute($statement);
        }
    }

    public function runQuery($cql, array $options = [])
    {
        $statement = $this->prepareStatement($cql);
        $rows =  $this->executeStatement($statement, $options);

        return static::rowsToArray($rows);
    }

    public static function rowsToArray($rows, array $options = [])
    {
        $array = [];
        foreach($rows as $row){
            $array[] = $row;
        }

        return $array;
    }
}