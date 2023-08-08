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
package org.neo4j.server.plugins;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.Representation;

/**
 * @deprecated Server plugins are deprecated for removal in the next major release. Please use unmanaged extensions instead.
 */
@Deprecated
public abstract class PluginPoint
{
    private final String name;
    private final Class<?> extendsType;
    private final String description;

    @Deprecated
    protected PluginPoint( Class<?> type, String name, String description )
    {
        this.extendsType = type;
        this.description = description == null ? "" : description;
        this.name = ServerPlugin.verifyName( name );
    }

    @Deprecated
    protected PluginPoint( Class<?> type, String name )
    {
        this( type, name, null );
    }

    @Deprecated
    public final String name()
    {
        return name;
    }

    @Deprecated
    public final Class<?> forType()
    {
        return extendsType;
    }

    @Deprecated
    public String getDescription()
    {
        return description;
    }

    @Deprecated
    public abstract Representation invoke( GraphDatabaseAPI graphDb, Object context,
            ParameterList params ) throws BadInputException, BadPluginInvocationException,
            PluginInvocationFailureException;

    @Deprecated
    protected void describeParameters( ParameterDescriptionConsumer consumer )
    {
    }
}
