<?php
namespace DreamFactory\Core\Cassandra\Components;

use DreamFactory\Core\Exceptions\InternalServerErrorException;

class CassandraClient
{
    protected $session = null;

    protected $keyspace = null;

    protected $schema = null;

    public function __construct(array $config)
    {
        $hosts = array_get($config, 'hosts');
        $port = array_get($config, 'port', 9042);
        $keyspace = array_get($config, 'keyspace');
        $username = array_get($config, 'username');
        $password = array_get($config, 'password');
        $ssl = $this->getSSLBuilder(array_get($config, 'options'));

        if(empty($hosts)){
            throw new InternalServerErrorException('No Cassandra host(s) provided in configuration.');
        }

        if(empty($keyspace)){
            throw new InternalServerErrorException('No Cassandra Keyspace provided in configuration.');
        }

        $cluster = \Cassandra::cluster()
                        ->withContactPoints($hosts)
                        ->withPersistentSessions(true)
                        ->withPort($port);

        if(!empty($username) && !empty($password)){
            $cluster->withCredentials($username, $password);
        }

        if(!empty($ssl)){
            $cluster->withSSL($ssl);
        }

        $this->session = $cluster->build()->connect($keyspace);
        $this->schema = $this->session->schema();
        $this->keyspace = $this->schema->keyspace($keyspace);
    }

    protected function getSSLBuilder($config)
    {
        if(empty($config)){
            return null;
        }

        $ssl = \Cassandra::ssl();
        $serverCert = array_get($config, 'server_cert_path');
        $clientCert = array_get($config, 'client_cert_path');
        $privateKey = array_get($config, 'private_key_path');
        $passPhrase = array_get($config, 'key_pass_phrase');

        if(!empty($serverCert) && !empty($clientCert)){
            if(empty($privateKey)){
                throw new InternalServerErrorException('No private key provider.');
            }
            return $ssl->withVerifyFlags(\Cassandra::VERIFY_PEER_CERT)
                        ->withTrustedCerts($serverCert)
                        ->withClientCert($clientCert)
                        ->withPrivateKey($privateKey, $passPhrase)
                        ->build();
        } elseif (!empty($serverCert)){
            return $ssl->withVerifyFlags(\Cassandra::VERIFY_PEER_CERT)
                        ->withTrustedCerts(getenv('SERVER_CERT'))
                        ->build();
        } elseif (true === boolval(array_get($config, 'ssl', array_get($config, 'tls', false)))){
            return $ssl->withVerifyFlags(\Cassandra::VERIFY_NONE)
                        ->build();
        } else {
            return null;
        }
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

    /**
     * Lists Cassandra table.
     * @return array
     */
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