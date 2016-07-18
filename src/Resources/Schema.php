<?php
namespace DreamFactory\Core\Cassandra\Resources;

use DreamFactory\Core\Cassandra\Components\Connection;
use DreamFactory\Core\Cassandra\Services\Cassandra;
use DreamFactory\Core\Contracts\SchemaInterface;
use DreamFactory\Core\Resources\BaseNoSqlDbSchemaResource;
use DreamFactory\Core\Contracts\RequestHandlerInterface;

class Schema extends BaseNoSqlDbSchemaResource
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var Connection
     */
    protected $dbConn = null;
    /**
     * @var SchemaInterface
     */
    protected $schema = null;


    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @param RequestHandlerInterface $parent
     */
    public function setParent(RequestHandlerInterface $parent)
    {
        parent::setParent($parent);

        /** @var Cassandra $parent */
        $this->dbConn = $parent->getConnection();

        /** @var Cassandra $parent */
        $this->schema = $parent->getSchema();
    }

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