<?php

class CassandraConfigTest extends \DreamFactory\Core\Database\Testing\DbServiceConfigTestCase
{
    protected $types = ['cassandra'];

    public function getDbServiceConfig($name, $type, $maxRecords = null)
    {
        $config = [
            'name'      => $name,
            'label'     => 'test db service',
            'type'      => $type,
            'is_active' => true,
            'config'    => [
                'hosts'     => 'localhost',
                'username' => 'user',
                'password' => 'secret',
                'keyspace' => 'keyspace'
            ]
        ];

        if (!empty($maxRecords)) {
            $config['config']['max_records'] = $maxRecords;
        }

        return $config;
    }
}