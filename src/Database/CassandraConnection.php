<?php
namespace DreamFactory\Core\Cassandra\Database;

use DreamFactory\Core\Cassandra\Database\Query\CassandraBuilder;
use DreamFactory\Core\Cassandra\Database\Query\Grammars\CassandraGrammar;
use DreamFactory\Core\Cassandra\Database\Query\Processors\CassandraProcessor;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
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

    /**
     * @return \DreamFactory\Core\Cassandra\Database\Query\Processors\CassandraProcessor
     */
    public function getDefaultPostProcessor()
    {
        return new CassandraProcessor();
    }

    /**
     * @return \DreamFactory\Core\Cassandra\Database\Query\Grammars\CassandraGrammar
     */
    public function getDefaultQueryGrammar()
    {
        return new CassandraGrammar();
    }

    /**
     * @return \DreamFactory\Core\Cassandra\Components\CassandraClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @return \Cassandra\Session|null
     */
    public function getSession()
    {
        return $this->client->getSession();
    }

    /**
     * @param string $table
     *
     * @return CassandraBuilder
     */
    public function table($table)
    {
        $processor = $this->getPostProcessor();
        $grammar = $this->getQueryGrammar();

        $query = new CassandraBuilder($this, $grammar, $processor);

        return $query->from($table);
    }

    /**
     * @param string $query
     * @param array  $bindings
     * @param bool   $useReadPdo
     *
     * @return mixed
     */
    public function select($query, $bindings = [], $useReadPdo = true)
    {
        $query .= ' ALLOW FILTERING';

        return $this->statement($query, $bindings);
    }

    /**
     * Run an insert statement against the database.
     *
     * @param  string $query
     * @param  array  $bindings
     *
     * @return bool
     * @throws InternalServerErrorException
     */
    public function insert($query, $bindings = [])
    {
        try {
            $this->statement($query, $bindings);

            return true;
        } catch (\Exception $e) {
            throw new InternalServerErrorException('Insert failed. ' . $e->getMessage());
        }
    }

    /**
     * Run an update statement against the database.
     *
     * @param  string $query
     * @param  array  $bindings
     *
     * @return bool
     * @throws InternalServerErrorException
     */
    public function update($query, $bindings = [])
    {
        try {
            $this->statement($query, $bindings);

            return true;
        } catch (\Exception $e) {
            throw new InternalServerErrorException('Update failed. ' . $e->getMessage());
        }
    }

    /**
     * Run a delete statement against the database.
     *
     * @param  string $query
     * @param  array  $bindings
     *
     * @return bool
     * @throws InternalServerErrorException
     */
    public function delete($query, $bindings = [])
    {
        try {
            $this->statement($query, $bindings);

            return true;
        } catch (\Exception $e) {
            throw new InternalServerErrorException('Update failed. ' . $e->getMessage());
        }
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param  string $query
     * @param  array  $bindings
     *
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        if (!empty($bindings)) {
            return $this->client->runQuery($query, ['arguments' => $bindings]);
        } else {
            return $this->client->runQuery($query);
        }
    }
}