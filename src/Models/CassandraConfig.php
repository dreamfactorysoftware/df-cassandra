<?php

namespace DreamFactory\Core\Cassandra\Models;

use DreamFactory\Core\Database\Components\SupportsExtraDbConfigs;
use DreamFactory\Core\Models\BaseServiceConfigModel;

class CassandraConfig extends BaseServiceConfigModel
{
    use SupportsExtraDbConfigs;

    protected $table = 'cassandra_config';

    protected $fillable = ['service_id', 'hosts', 'port', 'username', 'password', 'keyspace', 'options'];

    protected $casts = [
        'service_id' => 'integer',
        'port'       => 'integer',
        'options'    => 'array'
    ];

    protected $encrypted = ['password'];

    protected $protected = ['password'];

    /**
     * {@inheritdoc}
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'hosts':
                $schema['label'] = 'Host';
                $schema['default'] = '127.0.0.1';
                $schema['description'] =
                    'IP Address/Hostname of your Cassandra node. Note that you don’t have to specify the ' .
                    'addresses of all hosts in your cluster. Once the driver has established a connection to any ' .
                    'host, it will perform auto-discovery and connect to all hosts in the cluster';
                break;
            case 'port':
                $schema['label'] = 'Port';
                $schema['default'] = 9042;
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
            case 'options':
                $schema['type'] = 'object';
                $schema['object'] =
                    [
                        'key'   => ['label' => 'Name', 'type' => 'string'],
                        'value' => ['label' => 'Value', 'type' => 'string']
                    ];
                $schema['description'] =
                    'An array of options for the Cassandra connection.' .
                    ' Available options are - <br>' .
                    ' - ssl : boolean <br>' .
                    ' - server_cert_path : string <br>' .
                    ' - client_cert_path : string <br>' .
                    ' - private_Key_path : string <br>' .
                    ' - key_pass_phrase : string';
                break;
        }
    }
}