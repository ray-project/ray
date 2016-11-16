import React from 'react';
import {render} from 'react-dom';
import {AutoSizer, InfiniteLoader, List} from 'react-virtualized';
import classNames from 'classnames/bind';
import io from 'socket.io-client';
import scrollbarSize from 'dom-helpers/util/scrollbarSize';

class RayUI extends React.Component {

    constructor(props) {
      super(props);
      this.state = {
        table_one: "object_table",
        table_two: "task_table",
        table_three: "failure_table",
        table_four: "Remote_table",
        table_one_channel:"object",
        table_two_channel:"task",
        table_three_channel:"failure",
        table_four_channel:"remote",
        websocket_connection: io()
      };
    }

    render() {
      return (
        <div>
          <TableView table={this.state.table_one} channel={this.state.table_one_channel} socket={this.state.websocket_connection}/>
          <TableView table={this.state.table_two} channel={this.state.table_two_channel} socket={this.state.websocket_connection}/>
          <TableView table={this.state.table_three} channel={this.state.table_three_channel} socket={this.state.websocket_connection}/>
          <TableView table={this.state.table_four} channel={this.state.table_four_channel} socket={this.state.websocket_connection}/>
        </div>
      );
    }
}
class TableView extends React.Component {

    constructor(props) {
      super(props);
      this.state= {
        press: false
      };
      this._toggle=this._toggle.bind(this)
    }

    _toggle(e){
      this.setState({press: !this.state.press});
    }

    render() {
      return (
        <div>
          <button onClick={this._toggle}>{this.props.table}</button>
          <TableScroll press={this.state.press} table={this.props.table} channel={this.props.channel} socket={this.props.socket}/>
        </div>
      );
    }
}
class TableScroll extends React.Component{
    constructor(props) {
      super(props);
      this.state = {
        messages: [],
        filteredmsg: [],
        loadedRowsMap: {},
        content: "This is the view pane.",
        atEnd: true,
        currentpos: 0,
        filter: ""
      };
      this._onfilter = this._onfilter.bind(this);
      this.filterdata = this.filterdata.bind(this);
      this._isRowLoaded = this._isRowLoaded.bind(this);
      this._rowRenderer = this._rowRenderer.bind(this);
      this._loadMoreRows = this._loadMoreRows.bind(this);
      this.objectselect = this.objectselect.bind(this);
      this._objectrenderer = this._objectrenderer.bind(this);
      this.taskselect = this.taskselect.bind(this);
      this._taskrenderer = this._taskrenderer.bind(this);
      this.computationselect = this.computationselect.bind(this);
      this._computationrenderer = this._computationrenderer.bind(this);
      this.failureselect = this.failureselect.bind(this);
      this._failurerenderer = this._failurerenderer.bind(this);
      this.remoteselect = this.remoteselect.bind(this);
      this._remoterenderer = this._remoterenderer.bind(this);
      this.scrollcontroller = this.scrollcontroller.bind(this);
      switch (this.props.channel) {
        case "object": this.renderfunction = this._objectrenderer; 
                       this.header = (<div className={classNames("evenRow", "cell", "centeredCell")}>
                                        <div className={classNames("evenRow", "cell", "centeredCell")}>{"Object ID"}</div> 
                                        <div className={classNames("evenRow", "cell", "centeredCell")}>{"Plasma Store ID"}</div> 
                                      </div>); 
          break;
        case "failure": this.renderfunction = this._failurerenderer; 
                        this.header = (<div className={classNames("evenRow", "cell", "centeredCell")}> 
                                         <div className={classNames("evenRow", "cell", "centeredCell")}>{"Failed Function"}</div> 
                                       </div>); 
          break;
        case "computation": this.renderfunction = this._computationrenderer; 
                            this.header = (<div className={classNames("evenRow", "cell", "centeredCell")}> 
                                             <div className={classNames("evenRow", "cell", "centeredCell")}>{"Task iid"}</div> 
                                             <div className={classNames("evenRow", "cell", "centeredCell")}>{"Function_id"}</div> 
                                           </div>); 
          break;
        case "task": this.renderfunction = this._taskrenderer; 
                     this.header = (<div className={classNames("evenRow", "cell", "centeredCell")}> 
                                      <div className={classNames("evenRow", "cell", "centeredCell")}>{"Node id"}</div> 
                                      <div className={classNames("evenRow", "cell", "centeredCell")}>{"Function_id"}</div> 
                                    </div>); 
          break;
        case "remote": this.renderfunction = this._remoterenderer; 
                     this.header = (<div className={classNames("evenRow", "cell", "centeredCell")}> 
                                      <div className={classNames("evenRow", "cell", "centeredCell")}>{"Function id"}</div> 
                                      <div className={classNames("evenRow", "cell", "centeredCell")}>{"Module"}</div> 
                                      <div className={classNames("evenRow", "cell", "centeredCell")}>{"Function Name"}</div> 
                                    </div>); 
          break;
        default: break;
      }
    }
    componentDidMount() {
      var self = this;
      console.log("port" + location.port);
      this.props.socket.emit('getall', {table:this.props.channel});
      var arr = [];
      this.props.socket.on(this.props.channel, function(message) {
        console.log("got message " + message);
        var filteredarray = self.state.filteredmsg;
        message.forEach(function(msg,i){
 //         console.log("Content is " + JSON.stringify(msg));
          arr.push(msg);
          if (self.filterdata(msg, self.state.filter)) {filteredarray.push(msg);}
        });
        self.setState({messages: arr, filteredmsg: filteredarray});
      });
    }
    filterdata(data, filter) {
      console.log(data);
      var self = this;
      return filter != "" ? Object.values(data).filter(function(data){return data === Object(data) ? self.filterdata(data, filter) : String(data).includes(filter)}).length != 0 : true;
    }
    _onfilter(e) {
      var self = this;
      this.setState({filteredmsg: e.target.value != "" ? self.state.messages.filter(function(data){return self.filterdata(data, e.target.value)}) : self.state.messages, filter:e.target.value}); 
    }
    _isRowLoaded({ index }) {
      return !!this.state.loadedRowsMap[index];
    }
    objectselect(data, e) {
      console.log(data);
      this.setState({content:JSON.stringify(data)})
    }
    _objectrenderer(record, key, style) {
      const className = classNames("evenRow", "cell", "centeredCell");
      return (
        <button
          onClick={this.objectselect.bind(null, record)}
          className={classNames("evenRow", "cell", "rowbutton", "centeredCell")}
          key={key}
          style={style}
        >
          <div className={className}>{record.ObjectId}</div>
          <div className={className}>{record.PlasmaStoreId}</div>
        </button>
      );

    }   
    failureselect(data, e) {
      console.log(data);
      this.setState({content:data.error})
    }
    _failurerenderer(record, key, style) {
      const className = classNames("evenRow", "cell", "centeredCell");
      return (
        <button
          onClick={this.failureselect.bind(null, record)}
          className={classNames("evenRow", "cell", "rowbutton", "centeredCell")}
          key={key}
          style={style}
        >
          <div className={className}>{record.functionname}</div>
        </button>
      );

    }   
    computationselect(data, e) {
      console.log(data);
      this.setState({content: JSON.stringify});
    }

    _computationrenderer(record, key, style) {
      const className = classNames("evenRow", "cell", "centeredCell");
      return (
        <button
          onClick={this.computationselect.bind(null, record)}
          className={classNames("evenRow", "cell", "rowbutton", "centeredCell")}
          key={key}
          style={style}
        >
          <div className={className}>{record.task_iid.toString().substring(0,8)}</div>
          <div className={className}>{record.function_id.toString().substring(0,8)}</div>
        </button>
      );
    }   
    taskselect(data, e) {
      console.log(data);
      this.setState({content: JSON.stringify(data)});
    }

    _taskrenderer(record, key, style) {
      const className = classNames("evenRow", "cell", "centeredCell");
      return (
        <button
          onClick={this.taskselect.bind(null, record)}
          className={classNames("evenRow", "cell", "rowbutton", "centeredCell")}
          key={key}
          style={style}
        >
          <div className={className}>{record.node_id}</div>
          <div className={className}>{record.function_id.toString().substring(0,8)}</div>
        </button>
      );

    }   
    remoteselect(data, e) {
      console.log(data);
      this.setState({content: JSON.stringify(data)});
    }

    _remoterenderer(record, key, style) {
      const className = classNames("evenRow", "cell", "centeredCell");
      return (
        <button
          onClick={this.remoteselect.bind(null, record)}
          className={classNames("evenRow", "cell", "rowbutton", "centeredCell")}
          key={key}
          style={style}
        >
          <div className={className}>{record.function_id}</div>
          <div className={className}>{record.module}</div>
          <div className={className}>{record.name}</div>
        </button>
      );
    }   


    _rowRenderer({ index, key, style, isScrolling}){
      const record = this.state.filteredmsg[index];
      return this.renderfunction(record, key, style);
    }

    _loadMoreRows({ startIndex, stopIndex }) {
      for (var i = startIndex; i <= stopIndex; i++) {
        this.state.loadedRowsMap[i] = 1;
      }
      let promiseResolver;
      return new Promise(resolve => {
        promiseResolver = resolve;
      })
    }

    scrollcontroller({clientHeight, scrollHeight, scrollTop}){
      this.setState({atEnd:scrollTop >= scrollHeight- clientHeight, currentpos: Math.floor(scrollTop/20)-1});
    }

    render() {
      if (!this.props.press) {
        var style = {display:'none'}
      }
      var self = this;
      return (
        <AutoSizer disableHeight>
         {({ width }) => (
          <div style={style}>
           <input type="text" onChange={this._onfilter} />
           <div style={{width:width-2*scrollbarSize(), height:20}}>{this.header}</div>
           <InfiniteLoader
            isRowLoaded={this._isRowLoaded}
            loadMoreRows={this._loadMoreRows}
            rowCount={this.state.filteredmsg.length}
            style={{display: "inline-block"}}>
            {({ onRowsRendered, registerChild }) => (
                <List
                ref={registerChild}
                rowRenderer={this._rowRenderer}
                onRowsRendered={onRowsRendered}
                className={"BodyGrid"}
                width={width}
                height={300}
                onScroll={this.scrollcontroller}
                overscanRowCount={20}
                scrollToIndex={this.state.atEnd ? this.state.filteredmsg.length-1: this.currentpos}
                scrollToAlignment={"end"}
                rowCount={this.state.filteredmsg.length}
                rowHeight={20}
                />
              )}
           </InfiniteLoader>
           <textarea style={{width:width, height:300, display: "inline-block"}} value={this.state.content}></textarea>
          </div>
        )}
        </AutoSizer>
      );}
}
render(<RayUI/>, document.getElementById('mount-point'));
