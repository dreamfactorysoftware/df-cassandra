<?php
namespace DreamFactory\Core\Cassandra;

use DreamFactory\Core\Cassandra\Database\CassandraConnection;
use DreamFactory\Core\Cassandra\Models\CassandraConfig;
use DreamFactory\Core\Cassandra\Services\Cassandra;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use Illuminate\Database\DatabaseManager;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        $this->app->resolving('db', function (DatabaseManager $db) {
            $db->extend('cassandra', function ($config) {
                return new CassandraConnection($config);
            });
        });

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'cassandra',
                    'label'           => 'Cassandra',
                    'description'     => 'Database service for Cassandra connections.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'config_handler'  => CassandraConfig::class,
                    'factory'         => function ($config) {
                        return new Cassandra($config);
                    },
                ])
            );
        });
    }

    public function boot()
    {
        // add migrations
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }
}