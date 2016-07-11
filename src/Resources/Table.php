<?php
namespace DreamFactory\Core\Cassandra\Resources;

use DreamFactory\Core\Resources\BaseNoSqlDbTableResource;

class Table extends BaseNoSqlDbTableResource
{
    public function getIdsInfo(
        $table,
        $fields_info = null,
        &$requested_fields = null,
        $requested_types = null
    ){
        // TODO: Implement getIdsInfo() method.
    }
    
    public function rollbackTransaction()
    {
        // TODO: Implement rollbackTransaction() method.
    }
    
    public function commitTransaction($extras = null)
    {
        // TODO: Implement commitTransaction() method.
    }
    
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        // TODO: Implement retrieveRecordsByFilter() method.
    }
}