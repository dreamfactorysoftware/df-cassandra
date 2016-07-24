<?php
namespace DreamFactory\Core\Cassandra\Database;

use DreamFactory\Core\Cassandra\Database\Query\CassandraBuilder;
use DreamFactory\Core\Cassandra\Database\Query\Grammars\CassandraGrammar;
use DreamFactory\Core\Cassandra\Database\Query\Processors\CassandraProcessor;
use Illuminate\Database\Connection as IlluminateConnection;
use DreamFactory\Core\Cassandra\Components\CassandraClient;

class CassandraConnection extends IlluminateConnection
{
    /** @type CassandraClient */
    protected $client;

    public function __construct(array $config)
    {
        $this->client = new CassandraClient($config);
        $this->useDefaultPostProcessor();
        $this->useDefaultQueryGrammar();
    }

    public function getDefaultPostProcessor()
    {
        return new CassandraProcessor();
    }

    public function getDefaultQueryGrammar()
    {
        return new CassandraGrammar();
    }

    public function getClient()
    {
        return $this->client;
    }

    public function getSession()
    {
        return $this->client->getSession();
    }

    public function table($table)
    {
        $processor = $this->getPostProcessor();
        $grammar = $this->getQueryGrammar();

        $query = new CassandraBuilder($this, $grammar, $processor);

        return $query->from($table);
    }

    public function select($query, $bindings = [], $useReadPdo = true)
    {
        $query .= ' ALLOW FILTERING';
        return $this->statement($query, $bindings);
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param  string  $query
     * @param  array   $bindings
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        if(!empty($bindings)) {
            return $this->client->runQuery($query, ['arguments' => $bindings]);
        } else {
            return $this->client->runQuery($query);
        }
    }
}