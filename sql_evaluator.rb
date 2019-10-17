#!/usr/bin/ruby
# frozen_string_literal: true

require 'json'
require 'pp'
class Table
    attr_accessor :columns
    attr_accessor :name
    attr_accessor :as

    def initialize(columns, name, as)
        @columns = []
        columns.each do |column|
            @columns << Column.new(name, type, as)
        end
        @name = name
        @as = as
    end
end

class Query
end

class Column
    attr_accessor :name
    attr_accessor :type
    attr_accessor :as
end

class Where
    attr_accessor :op
    attr_accessor :left
    attr_accessor :right
end

def cross_product(tables) 
    table_data = []
    table_names = []
    tables.each do |table|
        contents = File.read(table[:file])
        table_data << Kernel.eval(contents)
        table_names << table[:name]
    end
    headers = []
    table_data.each_with_index do |td, idx|
        # headers = headers + [{table_names[idx] => td.shift}]
        headers = headers + td.shift
    end
    cross_product = table_data[0].product(*table_data[1..-1])
    cross_product = cross_product.map {|cp| cp.flatten}
    cross_product = [headers] + cross_product
end

table_folder = ARGV[0]
sql_json = File.read(ARGV[1])
sql_hash = JSON.parse(sql_json)
output = ARGV[2]

tables = sql_hash['from'].map{|x| x['source']}
table_files = tables.map{|table| {name: table, file: "#{table_folder}/#{table}.table.json"} }

cross_prod = cross_product(table_files)
# PP.pp cross_prod

wheres = sql_hash['where']
selected_columns = sql_hash['select'].map{|x| x['column']}
if wheres.empty?
    wheres = selected_columns
end
selected_columns = wheres & selected_columns
selected_columns = selected_columns.map {|col| col['table'].nil? ? "#{col['name']}" : "#{col['table']}_#{col['name']}"}

selected_tables = sql_hash['select'].map{|x| x['column']['table']}
# puts wheres
# puts columns
# PP.pp sql_hash['select']



headers = []
idx = 0
# puts "*****"
# cross_prod[0].each do |columns|
#     puts "----"
#     # columns.values.each do |col|
#     flat_cols = columns.values.flatten
#     PP.pp flat_cols
#     puts "----"
#     (0..flat_cols.length).step(2).each do |i|
#         headers << [selected_columns[idx],flat_cols[i+1]]
#         idx += 1
#     end
#     PP.pp headers
# end
cross_prod[0].each_with_index do |col, idx|
    headers << [selected_columns[idx], col[1]]
end
# puts "*****"
results = [headers] + cross_prod[1..-1]
File.write(output, results)

PP.pp results
