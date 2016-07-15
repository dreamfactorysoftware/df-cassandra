<?php
namespace DreamFactory\Core\Cassandra;

use DreamFactory\Core\Cassandra\Components\Connection;
use DreamFactory\Core\Cassandra\Models\CassandraConfig;
use DreamFactory\Core\Cassandra\Services\Cassandra;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        $this->app->resolving('db', function ($db) {
            $db->extend('cassandra', function ($config) {
                return new Connection($config);
            });
        });
        
        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df){
            $df->addType(
                new ServiceType([
                    'name'           => 'cassandra',
                    'label'          => 'Cassandra',
                    'description'    => 'Database service for Cassandra connections.',
                    'group'          => ServiceTypeGroups::DATABASE,
                    'config_handler' => CassandraConfig::class,
                    'factory'        => function ($config){
                        return new Cassandra($config);
                    },
                ])
            );
        });
    }
}