<?php
namespace DreamFactory\Core\Cassandra\Components;


class Connection extends \Illuminate\Database\Connection
{
    /** @type CassandraClient */
    protected $connection;

    public function __construct(array $config)
    {
        $this->connection = new CassandraClient($config);
    }
}