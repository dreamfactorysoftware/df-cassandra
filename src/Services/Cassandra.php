<?php
namespace DreamFactory\Core\Cassandra\Services;

use DreamFactory\Core\Cassandra\Database\Schema\Schema;
use DreamFactory\Core\Cassandra\Resources\Table;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Database\Services\BaseDbService;
use Illuminate\Database\DatabaseManager;

class Cassandra extends BaseDbService
{
    use RequireExtensions;

    public function __construct(array $settings)
    {
        parent::__construct($settings);

        $this->config['driver'] = 'cassandra';

        $prefix = '';
        $parts = ['hosts', 'port', 'username', 'keyspace'];
        foreach ($parts as $part) {
            $prefix .= array_get($this->config, $part);
        }

        $this->setConfigBasedCachePrefix($prefix . ':');
    }

    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();

        $handlers[Table::RESOURCE_NAME] = [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Table',
        ];

        return $handlers;
    }

    protected function initializeConnection()
    {
        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $this->config]);
        /** @type DatabaseManager $db */
        $db = app('db');
        $this->dbConn = $db->connection('service.' . $this->name);
        $this->schema = new Schema($this->dbConn);
    }
}