/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v3.runtime;

import java.time.Clock;

import org.neo4j.bolt.v1.runtime.CypherAdapterStream;
import org.neo4j.cypher.result.QueryResult;

import static org.neo4j.values.storable.Values.longValue;

class CypherAdapterStreamV3 extends CypherAdapterStream
{
    private static final String LAST_RESULT_CONSUMED_KEY = "t_last";

    CypherAdapterStreamV3( QueryResult delegate, Clock clock )
    {
        super( delegate, clock );
    }

    @Override
    protected void addRecordStreamingTime( Visitor visitor, long time )
    {
        visitor.addMetadata( LAST_RESULT_CONSUMED_KEY, longValue( time ) );
    }
}
