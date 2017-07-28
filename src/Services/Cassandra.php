<?php
namespace DreamFactory\Core\Cassandra\Services;

use DreamFactory\Core\Cassandra\Database\Schema\Schema;
use DreamFactory\Core\Cassandra\Resources\Table;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Database\Resources\DbSchemaResource;
use DreamFactory\Core\Database\Services\BaseDbService;
use DreamFactory\Core\Utility\Session;
use Illuminate\Database\DatabaseManager;

class Cassandra extends BaseDbService
{
    use RequireExtensions;

    /**
     * @var array
     */
    protected static $resources = [
        DbSchemaResource::RESOURCE_NAME => [
            'name'       => DbSchemaResource::RESOURCE_NAME,
            'class_name' => DbSchemaResource::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME  => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Table',
        ],
    ];

    public function __construct(array $settings)
    {
        parent::__construct($settings);

        $this->config['driver'] = 'cassandra';
    }

    protected function initializeConnection()
    {
        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $this->config]);
        /** @type DatabaseManager $db */
        $db = app('db');
        $this->dbConn = $db->connection('service.' . $this->name);
        $this->schema = new Schema($this->dbConn);
        $this->schema->setCache($this);
        $this->schema->setExtraStore($this);
    }
}