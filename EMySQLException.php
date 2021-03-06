<?php

/** @license
 *  Copyright 2009-2011 Rafael Gutierrez Martinez
 *  Copyright 2012-2013 Welma WEB MKT LABS, S.L.
 *  Copyright 2014-2016 Where Ideas Simply Come True, S.L.
 *  Copyright 2017 nabu-3 Group
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

namespace providers\mysql\driver;

use Exception;
use \nabu\db\exceptions\ENabuDBException;

/**
 * @author Rafael Gutierrez <rgutierrez@nabu-3.com>
 * @since 0.0.1
 * @version 0.0.1
 * @package providers\mysql\driver
 */
class EMySQLException extends ENabuDBException
{
    /* Query error messages */
    /** @var int */
    const ERROR_QUERY_RESULTSET_NOT_FOUND = 0x8001;
    /** @var int */
    const ERROR_QUERY_RESULTSET_NOT_ALLOWED = 0x8002;
    /** @var int */
    const ERROR_QUERY_INVALID_SENTENCE = 0x8003;

    /* Syntax Builder error messages */
    /** @var int */
    const ERROR_SYNTAX_TABLE_NAME_NOT_FOUND = 0xc001;

    /** @var int */
    const ERROR_SYNTAX_FIELDS_NOT_FOUND = 0xc101;
    /** @var int */
    const ERROR_SYNTAX_FIELD_NAME_NOT_FOUND = 0xc102;

    /** @var int */
    const ERROR_SYNTAX_INVALID_CONSTRAINT_DESCRIPTOR = 0xc301;
    /** @var int */
    const ERROR_SYNTAX_CONSTRAINT_ATTR_NOT_FOUND = 0xc302;
    /** @var int */
    const ERROR_SYNTAX_CONSTRAINT_FIELDS_NOT_FOUND = 0xc303;
    /** @var int */
    const ERROR_SYNTAX_CONSTRAINT_FIELDS_EMPTY = 0xc304;

    public function __construct(
        int $code,
        int $sql_code = 0,
        string $sql_message = null,
        string $sql_script = null,
        array $values = null,
        Exception $previous = null
    ) {
        $this->overloadMessages();

        parent::__construct($code, $sql_code, $sql_message, $sql_script, $values, $previous);
    }

    protected function overloadMessages()
    {
        /* Query error messages */
        $this->error_messages[EMySQLException::ERROR_QUERY_RESULTSET_NOT_FOUND] =
            'MySQL Query resultset not found';
        $this->error_messages[EMySQLException::ERROR_QUERY_RESULTSET_NOT_ALLOWED] =
            'MySQL Query does not support resultset';
        $this->error_messages[EMySQLException::ERROR_QUERY_INVALID_SENTENCE] =
            'MySQL Invalid sentence';

        /* Syntax Builder error messages */
        $this->error_messages[EMySQLException::ERROR_SYNTAX_TABLE_NAME_NOT_FOUND] =
            'Descriptor does not contain a table name';
        $this->error_messages[EMySQLException::ERROR_SYNTAX_FIELDS_NOT_FOUND] =
            'Descriptor does not contain fields descriptions';
        $this->error_messages[EMySQLException::ERROR_SYNTAX_FIELD_NAME_NOT_FOUND] =
            'Field descriptor does not contain the field name';
        $this->error_messages[EMySQLException::ERROR_SYNTAX_INVALID_CONSTRAINT_DESCRIPTOR] =
            'Invalid constraint descriptor';
        $this->error_messages[EMySQLException::ERROR_SYNTAX_CONSTRAINT_ATTR_NOT_FOUND] =
            'Constraint descriptor does not contain attribute [%s]';
        $this->error_messages[EMySQLException::ERROR_SYNTAX_CONSTRAINT_FIELDS_NOT_FOUND] =
            'Fields array not found in constraint descriptor [%s]';
        $this->error_messages[EMySQLException::ERROR_SYNTAX_CONSTRAINT_FIELDS_EMPTY] =
            'Fields array is empty in constraint descriptor [%s]';
    }
}
