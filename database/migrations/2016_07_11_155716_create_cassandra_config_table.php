<?php

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Migrations\Migration;

class CreateCassandraConfigTable extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        //Create Cassandra config table
        Schema::create(
            'cassandra_config',
            function (Blueprint $t){
                $t->integer('service_id')->unsigned()->primary();
                $t->foreign('service_id')->references('id')->on('service')->onDelete('cascade');
                $t->string('hosts');
                $t->string('port')->nullable();
                $t->string('username')->nullable();
                $t->longText('password')->nullable();
                $t->string('keyspace');
            }
        );
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        //Drop Cassandra config table
        Schema::dropIfExists('cassandra_config');
    }
}
