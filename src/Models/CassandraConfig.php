<?php
namespace DreamFactory\Core\Cassandra\Models;

use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Models\BaseServiceConfigModel;

class CassandraConfig extends BaseServiceConfigModel
{
    use RequireExtensions;

    protected $table = 'cassandra_config';

    protected $fillable = ['service_id', 'hosts', 'port', 'username', 'password', 'keyspace'];

    protected $casts = [
        'service_id' => 'integer'
    ];

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'hosts':
                $schema['label'] = 'Host(s)';
                $schema['default'] = 'localhost';
                $schema['description'] =
                    'IP Addresses/Hostnames of your Cassandra nodes. Note that you don’t have to specify the ' .
                    'addresses of all hosts in your cluster. Once the driver has established a connection to any ' .
                    'host, it will perform auto-discovery and connect to all hosts in the cluster';
                break;
            case 'port':
                $schema['label'] = 'Port';
                $schema['default'] = '9042';
                $schema['description'] = 'Cassandra Port number';
                break;
            case 'username':
                $schema['label'] = 'Username';
                $schema['description'] = 'Cassandra User';
                break;
            case 'password':
                $schema['label'] = 'Password';
                $schema['description'] = 'User Password';
                break;
            case 'keyspace':
                $schema['label'] = 'Keyspace';
                $schema['description'] = 'Keyspace/Namespace of your Cassandra tables';
                break;
        }
    }
}