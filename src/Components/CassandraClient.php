<?php
namespace DreamFactory\Core\Cassandra\Components;

use DreamFactory\Core\Exceptions\InternalServerErrorException;

class CassandraClient
{
    protected $session = null;

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
    }
    
    public function getSession()
    {
        return $this->session;
    }
}