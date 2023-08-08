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
package org.neo4j.kernel.builtinprocs;

import org.neo4j.configuration.ConfigValue;

import static org.apache.commons.lang3.StringUtils.EMPTY;

public class ConfigResult
{
    public final String name;
    public final String description;
    public final String value;
    public final boolean dynamic;

    ConfigResult( ConfigValue configValue )
    {
        this.name = configValue.name();
        this.description = configValue.description().orElse( EMPTY );
        this.value = configValue.valueAsString().orElse( EMPTY );
        this.dynamic = configValue.dynamic();
    }
}
