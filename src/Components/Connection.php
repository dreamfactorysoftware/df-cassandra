<?php
namespace DreamFactory\Core\Cassandra\Components;


class Connection extends \Illuminate\Database\Connection
{
    /** @type CassandraClient */
    protected $client;

    public function __construct(array $config)
    {
        $this->client = new CassandraClient($config);
    }

    public function getClient()
    {
        return $this->client;
    }

    public function getSession()
    {
        return $this->client->getSession();
    }
}