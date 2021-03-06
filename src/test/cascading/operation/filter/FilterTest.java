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

package cascading.operation.filter;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class FilterTest extends CascadingTestCase
  {
  private ConcreteCall operationCall;

  public FilterTest()
    {
    super( "filter test" );
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();
    operationCall = new ConcreteCall();
    }

  private TupleEntry getEntry( Tuple tuple )
    {
    return new TupleEntry( Fields.size( tuple.size() ), tuple );
    }

  public void testNotNull()
    {
    Filter filter = new FilterNotNull();

    operationCall.setArguments( getEntry( new Tuple( 1 ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );
    operationCall.setArguments( getEntry( new Tuple( (Comparable) null ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );


    operationCall.setArguments( getEntry( new Tuple( "0", 1 ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );
    operationCall.setArguments( getEntry( new Tuple( "0", null ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );
    operationCall.setArguments( getEntry( new Tuple( null, null ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );
    }

  public void testNull()
    {
    Filter filter = new FilterNull();

    operationCall.setArguments( getEntry( new Tuple( 1 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );
    operationCall.setArguments( getEntry( new Tuple( (Comparable) null ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );

    operationCall.setArguments( getEntry( new Tuple( "0", 1 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );
    operationCall.setArguments( getEntry( new Tuple( "0", null ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );
    operationCall.setArguments( getEntry( new Tuple( null, null ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );
    }

  public class BooleanFilter extends BaseOperation implements Filter
    {
    private final boolean result;

    public BooleanFilter( boolean result )
      {
      this.result = result;
      }

    public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
      {
      return result;
      }
    }

  public void testAnd()
    {
    Fields[] fields = new Fields[]{new Fields( 0 ), new Fields( 1 )};

    Filter[] filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( true )};
    Filter filter = new And( fields, filters );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( false )};
    filter = new And( fields, filters );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( true )};
    filter = new And( fields, filters );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( false )};
    filter = new And( fields, filters );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );
    }

  public void testOr()
    {
    Fields[] fields = new Fields[]{new Fields( 0 ), new Fields( 1 )};

    Filter[] filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( true )};
    Filter filter = new Or( fields, filters );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( false )};
    filter = new Or( fields, filters );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( true )};
    filter = new Or( fields, filters );


    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( false )};
    filter = new Or( fields, filters );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );
    }

  public void testXor()
    {
    Fields[] fields = new Fields[]{new Fields( 0 ), new Fields( 1 )};

    Filter[] filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( true )};
    Filter filter = new Xor( fields[ 0 ], filters[ 0 ], fields[ 1 ], filters[ 1 ] );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( false )};
    filter = new Xor( fields[ 0 ], filters[ 0 ], fields[ 1 ], filters[ 1 ] );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( true )};
    filter = new Xor( fields[ 0 ], filters[ 0 ], fields[ 1 ], filters[ 1 ] );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( false )};
    filter = new Xor( fields[ 0 ], filters[ 0 ], fields[ 1 ], filters[ 1 ] );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );
    }

  public void testNot()
    {
    Fields[] fields = new Fields[]{new Fields( 0 ), new Fields( 1 )};

    Filter[] filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( true )};
    Filter filter = new Not( new Or( fields, filters ) );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( false )};
    filter = new Not( new Or( fields, filters ) );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( true )};
    filter = new Not( new Or( fields, filters ) );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertFalse( filter.isRemove( null, operationCall ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( false )};
    filter = new Not( new Or( fields, filters ) );

    filter.prepare( FlowProcess.NULL, operationCall );
    operationCall.setArguments( getEntry( new Tuple( 1, 2 ) ) );
    assertTrue( filter.isRemove( null, operationCall ) );
    }
  }