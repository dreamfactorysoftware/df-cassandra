<?php
namespace DreamFactory\Core\Cassandra\Components;

use DreamFactory\Core\Exceptions\InternalServerErrorException;

class CassandraClient
{
    protected $session = null;

    protected $keyspace = null;

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
                        ->withPort($port);

        if(!empty($username) && !empty($password)){
            $cluster->withCredentials($username, $password);
        }

        $this->session = $cluster->build()->connect($keyspace);
        $this->keyspace = $keyspace;
    }
    
    public function getSession()
    {
        return $this->session;
    }

    public function getKeyspace()
    {
        return $this->keyspace;
    }

    public function listTables()
    {
        //$statement = new \Cassandra\SimpleStatement("select table_name from system_schema.tables where keyspace_name = 'df2'");
        /** @noinspection PhpUndefinedNamespaceInspection */
        $statement = new \Cassandra\SimpleStatement(<<<CQL
select table_name from system_schema.tables where keyspace_name = '$this->keyspace'
CQL
);
        $result = $this->session->execute($statement);

        return $result;
    }
}