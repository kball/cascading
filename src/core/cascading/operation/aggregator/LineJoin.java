/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.operation.aggregator;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;

/** Class LineJoin is an {@link Aggregator} that returns the sum of all numeric values in the current group. */
public class LineJoin extends BaseOperation<String[]> implements Aggregator<String[]>
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "line_join";

  private String lineDelimiter = "";
  private String fieldDelimiter = "";

  /** Constructor LineJoin creates a new LineJoin instance that accepts one argument and returns a single field named "line_join". */
  public LineJoin()
    {
    super( 1, new Fields( FIELD_NAME ) );
    }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts
   * only 1 argument.
   *
   * @param fieldDeclaration of type Fields
   */
  public LineJoin( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts
   * only 1 argument. 
   *
   * @param fieldDeclaration of type Fields
   * @param lineDelimiter    of type String
   * @param fieldDelimiter   of type String
   */
  public LineJoin( Fields fieldDeclaration, String lineDelimiter, String fieldDelimiter )
    {
    this( fieldDeclaration );
    this.lineDelimiter = lineDelimiter;
    this.fieldDelimiter = fieldDelimiter;
    }

  public void start( FlowProcess flowProcess, AggregatorCall<String[]> aggregatorCall )
    {
    if( aggregatorCall.getContext() == null )
      aggregatorCall.setContext( new String[]{""} );
    else
      aggregatorCall.getContext()[ 0 ] = "";
    }

  public void aggregate( FlowProcess flowProcess, AggregatorCall<String[]> aggregatorCall )
    {
    if (aggregatorCall.getContext()[ 0 ].length() > 0) {
      aggregatorCall.getContext()[ 0 ] += lineDelimiter;
    }
    aggregatorCall.getContext()[ 0 ] += aggregatorCall.getArguments().getTuple().
					toString( fieldDelimiter );
    }

  public void complete( FlowProcess flowProcess, AggregatorCall<String[]> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  protected Tuple getResult( AggregatorCall<String[]> aggregatorCall )
    {
    return new Tuple( (String) aggregatorCall.getContext()[ 0 ] );
    }
  }
