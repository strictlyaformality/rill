const argv = require('yargs').argv;
const fs = require('fs');
const stream = require('stream');

class TypeMap {
    constructor() {
        this.map = {
            string: this.toString,
            boolean: this.toBoolean,
            number: this.toNumber,
            date: this.toDate
        };
    }

    mapValue(value, dataType){
        return this.map[dataType](value);
    }
    toString(value) {
        return String(value);
    }
    toBoolean(value) {
        return !!value;
    }
    toNumber(value) {
        return +value;
    }
    toDate(value) {
        return new Date(value);
    }
}

class CsvToJson {
    constructor(options){
        options = options || {};
        this.path = options.path;
        this.lineDelimiter = options.lineDelimiter || '\r\n';
        this.valueDelimiter = options.valueDelimiter || ',';
        this.parseHeaders = !!options.parseHeaders;
        this.headerTypes = options.headerTypes || [];
        this.toFile = options.toFile || false;
        this.outFile = options.outFile || './out.json';
        this.typeMapper = new TypeMap();
        this.headers = null;
        this.stub = null;
    }
    //get the header type, defaults to 'string'
    getHeaderType(index){
        return this.headerTypes[index] || 'string';
    }
    //parse data based on delimiter, return a string.
    parse(data, delimiter){
        return data
            .toString()
            .split(delimiter);
    }
    //parse chunk based on supplied delimiter
    parseChunk(chunk){
        return this.parse(chunk, this.lineDelimiter);
    }
    //parse line based on supplied delimiter
    parseLine(line){
        return this.parse(line, this.valueDelimiter)
                .reduce(function (obj, value, index) {
                    return Object.assign(obj, {
                        [this.headers[index]]: this.typeMapper.mapValue(value, this.getHeaderType(index))
                    });
                }.bind(this), {});
    }

    //get the chunk transform stream for piping
    _getChunkTransformStream(){
        //create a new transform stream
        let chunkTransform = new stream.Transform({objectMode: true});

        //tranform implementation
        chunkTransform._transform = function (chunk, encoding, cb) {
                //prepend the extracted line from the previous chunk
                if (this.stub) {
                    chunk = this.stub + chunk;
                }
                let lines = this.parseChunk(chunk);
                //take the last line of the chunk
                //in case it ends in the middle of a line
                this.stub = lines.splice(-1);
                //if headers need to be parsed
                if(this.parseHeaders && this.headers === null) {
                    this.headers = lines.splice(0, 1)[0].split(',');
                }
                //push each line to the write stream
                lines.forEach(chunkTransform.push.bind(chunkTransform));
                cb();
            }.bind(this);

        //flush implementation
        chunkTransform._flush = function (cb) {
            //if a stub exists...
            if(this.stub){
                //push the final line to the write stream
                chunkTransform.push(this.stub.toString());
            }
            //unset the stub
            this.stub = null;
            cb();
        }.bind(this);
        chunkTransform.on('finish', () => console.log(this.headers));
        return chunkTransform;
    }
    //get the line transform stream for piping
    _getLineTransformStream(){
        let lineTransform = new stream.Transform({objectMode: true});
        //transform implementation
        lineTransform._transform = function(line, encoding, cb){
            lineTransform.push(JSON.stringify(this.parseLine(line)));
            cb();
        }.bind(this);

        return lineTransform;
    }
    //stream to JSON file
    _getFileStream(){
        let fileTransform = new stream.Transform({objectMode: true});
        let queued = null;
        fileTransform._transform = function(line, encoding, cb){
            //queue the first line and do not push it
            if(!queued) {
                //queue the current line
                queued = line;
                //push the opening brackets
                fileTransform.push('{\n\t"data": [\n');
            } else {
                //push the previous line on with a comma
                fileTransform.push('\t\t' + queued + ',\n');
                //set the queued line to the current line
                queued = line;
            }
            cb();
        };
        fileTransform._flush = function(cb){
            //push the final line
            fileTransform.push('\t\t' + queued + '\n');
            //push the closing brackets
            fileTransform.push('\t]\n}');
            cb();
        };

        return fileTransform;
    }
    //get the composed stream
    getStream(){
        let stream = fs
            .createReadStream(this.path)
            .pipe(this.getChunkTransformStream())
            .pipe(this.getLineTransformStream());

        if (this.toFile){
            return stream
                .pipe(this.getFileStream())
                .pipe(fs.createWriteStream(this.outFile));
        } else {
            return stream;
        }
    }
}

module.exports = CsvToJson;

/*new CsvToJson({
    headerTypes: ['number', 'date', 'boolean'],
    path: argv.filePath,
    outFile: argv.outFile,
    toFile: argv.f,
    parseHeaders: argv.h
})
.getStream();*/

