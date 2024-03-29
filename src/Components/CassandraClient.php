<?php
namespace DreamFactory\Core\Cassandra\Components;

use DreamFactory\Core\Exceptions\InternalServerErrorException;
use Illuminate\Support\Arr;

class CassandraClient
{
    /** @var \Cassandra\Session|null */
    protected $session = null;

    /** @var \Cassandra\Keyspace|null */
    protected $keyspace = null;

    /** @var \Cassandra\Schema null */
    protected $schema = null;

    /**
     * CassandraClient constructor.
     *
     *
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    public function __construct(array $config)
    {
        $hosts = Arr::get($config, 'hosts');
        $port = Arr::get($config, 'port', 9042);
        $keyspace = Arr::get($config, 'keyspace');
        $username = Arr::get($config, 'username');
        $password = Arr::get($config, 'password');
        $ssl = $this->getSSLBuilder(Arr::get($config, 'options'));

        if (empty($hosts)) {
            throw new InternalServerErrorException('No Cassandra host(s) provided in configuration.');
        }

        if (empty($keyspace)) {
            throw new InternalServerErrorException('No Cassandra Keyspace provided in configuration.');
        }

        $cluster = \Cassandra::cluster()
            ->withContactPoints($hosts)
            ->withPersistentSessions(true)
            ->withPort($port);

        if (!empty($username) && !empty($password)) {
            $cluster->withCredentials($username, $password);
        }

        if (!empty($ssl)) {
            $cluster->withSSL($ssl);
        }

        $this->session = $cluster->build()->connect($keyspace);
        $this->schema = $this->session->schema();        
        $ks = $this->schema->keyspace($keyspace);
        if ($ks === false) {
            throw new \InvalidArgumentException("Keyspace $keyspace not found for cassandra connection");
        } else {
            $this->keyspace = $ks;
        }
    }

    /**
     * Creates the SSL connection builder.
     *
     * @param $config
     *
     * @return \Cassandra\SSLOptions|null
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function getSSLBuilder($config)
    {
        if (empty($config)) {
            return null;
        }

        $ssl = \Cassandra::ssl();
        $serverCert = Arr::get($config, 'server_cert_path');
        $clientCert = Arr::get($config, 'client_cert_path');
        $privateKey = Arr::get($config, 'private_key_path');
        $passPhrase = Arr::get($config, 'key_pass_phrase');

        if (!empty($serverCert) && !empty($clientCert)) {
            if (empty($privateKey)) {
                throw new InternalServerErrorException('No private key provider.');
            }

            return $ssl->withVerifyFlags(\Cassandra::VERIFY_PEER_CERT)
                ->withTrustedCerts($serverCert)
                ->withClientCert($clientCert)
                ->withPrivateKey($privateKey, $passPhrase)
                ->build();
        } elseif (!empty($serverCert)) {
            return $ssl->withVerifyFlags(\Cassandra::VERIFY_PEER_CERT)
                ->withTrustedCerts(getenv('SERVER_CERT'))
                ->build();
        } elseif (true === boolval(Arr::get($config, 'ssl', \Illuminate\Support\Arr::get($config, 'tls', false)))) {
            return $ssl->withVerifyFlags(\Cassandra::VERIFY_NONE)
                ->build();
        } else {
            return null;
        }
    }

    /**
     * @return \Cassandra\Session|null
     */
    public function getSession()
    {
        return $this->session;
    }

    /**
     * @return \Cassandra\Keyspace|null
     */
    public function getKeyspace()
    {
        return $this->keyspace;
    }

    /**
     * @return \Cassandra\Schema
     */
    public function getSchema()
    {
        return $this->schema;
    }

    /**
     * Lists Cassandra table.
     *
     * @return array
     */
    public function listTables()
    {
        $tables = $this->keyspace->tables();
        $out = [];
        foreach ($tables as $table) {
            $out[] = ['table_name' => $table->name()];
        }

        return $out;
    }

    /**
     * @param $name
     *
     * @return \Cassandra\Table
     */
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
     * @param \Cassandra\SimpleStatement $statement
     *
     * @return \Cassandra\Rows
     */
    public function executeStatement($statement, array $options = [])
    {
        if (!empty($options)) {
            return $this->session->execute($statement, $options);
        } else {
            return $this->session->execute($statement);
        }
    }

    /**
     * @param string $cql
     *
     * @return array
     */
    public function runQuery($cql, array $options = [])
    {
        $pageInfo = $this->extractPaginationInfo($cql);
        $statement = $this->prepareStatement($cql);
        $rows = $this->executeStatement($statement, $options);

        return static::rowsToArray($rows, $pageInfo);
    }

    /**
     * Extracts pagination info from CQL.
     *
     * @param $cql
     *
     * @return array
     */
    protected function extractPaginationInfo(& $cql)
    {
        $words = explode(' ', (string) $cql);
        $limit = 0;
        $offset = 0;
        $limitKey = null;
        foreach ($words as $key => $word) {
            if ('limit' === strtolower($word) && is_numeric($words[$key + 1])) {
                $limit = (int)$words[$key + 1];
                $limitKey = $key + 1;
            }
            if ('offset' === strtolower($word) && is_numeric($words[$key + 1])) {
                $offset = (int)$words[$key + 1];
                //Take out offset from CQL. It is not supported.
                unset($words[$key], $words[$key + 1]);
            }
        }

        //Offset is not supported by CQL. Therefore need to modify limit based on offset.
        //Adding offset to limit in order to fetch all available records.
        $limit += $offset;
        if ($limitKey !== null) {
            $words[$limitKey] = $limit;
        }
        $cql = implode(' ', $words);

        return ['limit' => $limit, 'offset' => $offset];
    }

    /**
     * @param \Cassandra\Rows $rows
     *
     * @return array
     */
    public static function rowsToArray($rows, array $options = [])
    {
        $limit = Arr::get($options, 'limit', 0);
        $offset = Arr::get($options, 'offset', 0);

        $array = [];
        if ($offset > 0) {
            if ($limit > 0) {
                for ($i = 0; ($i < $limit && $rows->offsetExists($offset + $i)); $i++) {
                    $array[] = $rows->offsetGet($offset + $i);
                }
            } else {
                for ($i = 0; $rows->offsetExists($offset + $i); $i++) {
                    $array[] = $rows->offsetGet($offset + $i);
                }
            }
        } else {
            foreach ($rows as $row) {
                $array[] = $row;
            }
        }

        return $array;
    }
}