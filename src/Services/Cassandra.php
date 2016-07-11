<?php
namespace DreamFactory\Core\Cassandra\Services;

use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Services\BaseNoSqlDbService;
use DreamFactory\Core\Cassandra\Resources\Schema;
use DreamFactory\Core\Cassandra\Resources\Table;

class Cassandra extends BaseNoSqlDbService implements CacheInterface, DbExtrasInterface
{
    use DbSchemaExtras, RequireExtensions;

    /**
     * @var array
     */
    protected static $resources = [
        Schema::RESOURCE_NAME => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME  => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Table',
        ],
    ];
    
    public function getTableNames($schema = null, $refresh = false, $use_alias = false)
    {
        // TODO: Implement getTableNames() method.
    }
    
    public function refreshTableCache()
    {
        // TODO: Implement refreshTableCache() method.
    }
    
    
}