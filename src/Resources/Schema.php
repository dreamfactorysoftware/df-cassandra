<?php
namespace DreamFactory\Core\Cassandra\Resources;

use DreamFactory\Core\Resources\BaseNoSqlDbSchemaResource;

class Schema extends BaseNoSqlDbSchemaResource
{
    public function describeTable($table, $refresh = false)
    {
        // TODO: Implement describeTable() method.
    }
    
    public function deleteTable($table, $check_empty = false)
    {
        // TODO: Implement deleteTable() method.
    }
    
    public function updateTable($table, $properties, $allow_delete_fields = false, $return_schema = false)
    {
        // TODO: Implement updateTable() method.
    }
    
    public function createTable($table, $properties = [], $check_exist = false, $return_schema = false)
    {
        // TODO: Implement createTable() method.
    }
}