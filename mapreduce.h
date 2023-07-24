#pragma once

#include <thread>
#include <vector>
#include <iostream>
#include <fstream>
#include <math.h>
#include <sstream>
#include <map>
#include <stdexcept>
#include <future>
#include <mutex>

/**
 * Это MapReduce фреймворк.
 * Он универсальный.
 * Он может выполнять разные map_reduce задачи.
 * Он просто обрабатывает какие-то данные с помощью каких-то функций в нескольких потоках.
 * Он ничего не знает о задаче, которую решает.
 * Здесь не должно быть кода, завязанного на конкретную задачу - определение длины префикса.
 * 
 * С помощью этого фреймворка должны решаться разные задачи.
 * Когда напишете это, попробуйте решить с помощью этого фреймворка все задачи, которые мы разбирали на лекции.
 * 
 * Это наш самописный аналог hadoop mapreduce.
 * Он на самом деле не работает с по-настоящему большими данными, потому что выполняется на одной машине.
 * Но мы делаем вид, что данных много, и представляем, что наши потоки - это процессы на разных узлах.
 * 
 * Ни один из потоков не должен полностью загружать свои данные в память или пробегаться по всем данным.
 * Каждый из потоков обрабатывает только свой блок.
 * 
 * На самом деле даже один блок данных не должен полностью грузиться в оперативку, а должен обрабатываться построчно.
 * Но в домашней работе можем этим пренебречь и загрузить один блок в память одним потоком.
 * 
 * Всё в этом файле - это рекомендация.
 * Если что-то будет слишком сложно реализовать, идите на компромисс, пренебрегайте чем-нибудь.
 * Лучше сделать что-нибудь, чем застрять на каком-нибудь моменте и не сделать ничего.
 */

using MapperFunction = std::function< std::pair< std::string, int >( const std::string& ) >;
using ReducerFunction = std::function< bool ( std::pair< std::string, int >& ) >;
using CombinerFunction = std::function< void ( const std::string&, int ) >;

class MapReduce
{
public:
    
    MapReduce( int mappers, int reducers )
    : mappers_count( mappers )
    , reducers_count( reducers )
    {}
    
    void set_mapper( MapperFunction function )
    {
        mapper = function;
    }
    
    void set_combiner( CombinerFunction function )
    {
        combiner = function;
    }
    
    void set_reducer( ReducerFunction function )
    {
        reducer = function;
    }
    
    void run( const std::filesystem::path& input, const std::filesystem::path& output, int prefix_length )
    {
        auto blocks = split_file(input, mappers_count);

        if( blocks.empty() )
            throw std::runtime_error("empty input file");
        
        // Создаём mappers_count потоков
        // В каждом потоке читаем свой блок данных
        // Применяем к строкам данных функцию mapper
        // Сортируем результат каждого потока
        // Результат сохраняется в файловую систему (представляем, что это большие данные)
        // Каждый поток сохраняет результат в свой файл (представляем, что потоки выполняются на разных узлах)
        
        std::vector< std::string > mapper_files;
        auto apply_map = [ &input, &mapper_files, this ]( Block block, int thread_num )
        {
            std::ifstream is( input.string(), std::ios::binary );
            std::string buff( block.to - block.from, '0' );
            is.seekg( block.from, is.beg );
            is.read( &buff[0], block.to - block.from );
            
            std::istringstream lines;
            lines.str( buff );
                      
            std::ofstream output( mapper_files[ thread_num ] );
            
            int idx = 0;
            for ( std::string line; getline( lines, line ); )
            {
                auto res = mapper( line );
                if( idx++ )
                    output << "\n";
                output << res.first << " " << res.second;
                output.flush();
            }
            
            output.close();
            
            combiner( mapper_files[ thread_num ], thread_num );
        };


        std::vector< std::thread > threads;
        for ( std::size_t i = 0; i < mappers_count; ++i )
        {
            std::stringstream filename_stream;
            filename_stream << "mapper" << i << ".txt";
            mapper_files.push_back( filename_stream.str() );
            threads.emplace_back( apply_map, blocks[ i ], i );
        }

        for ( auto& th : threads )
        {
            if ( th.joinable() )
                th.join();
        }

        // Создаём reducers_count новых файлов
        std::vector< std::string > reducer_files;
        std::vector< std::ofstream > reducer_write( reducers_count );
        std::vector< std::ifstream > mappers_read ( mappers_count );
        
        for( int i = 0; i < reducers_count; ++i )
        {
            std::stringstream filename_stream;
            filename_stream << "reducer" << i << ".txt";
            reducer_files.push_back( filename_stream.str() );
            reducer_write[i].open( filename_stream.str() );
        }

        // находим самый большой файл от мапперов
        int largest_file_idx = 0;
        std::uintmax_t largest_file_size =  std::filesystem::file_size( mapper_files[ 0 ] );
        for( int i = 1; i < static_cast< int >( mapper_files.size() ); ++i )
        {
            if( largest_file_size < std::filesystem::file_size( mapper_files[i] ) )
            {
                largest_file_idx = i;
                largest_file_size =  std::filesystem::file_size( mapper_files[ i ] );
            }
        }
        
        for ( std::size_t i = 0; i < mappers_count; ++i )
        {
            mappers_read[i].open( mapper_files[i] );
        }
        
        // Из mappers_count файлов читаем данные (результат фазы map) и перекладываем в reducers_count (вход фазы reduce)
        // Перекладываем так, чтобы:
        //     * данные были отсортированы
        //     * одинаковые ключи оказывались в одном файле, чтобы одинаковые ключи попали на один редьюсер
        //     * файлы примерно одинакового размера, чтобы редьюсеры были загружены примерно равномерно

        std::map< std::string, int > merge_data;
        
        int reducers_idx = 0;
        while( !mappers_read[ largest_file_idx ].eof() ) // читаем, пока не закончится самый большой файл
        {
            for ( int i = 0; i < mappers_count; ++i )
            {
                if( mappers_read[ i ].eof() )
                    continue;
                
                std::string key;
                int value;
                mappers_read[ i ] >> key >> value;
                
                merge_data[ key ] += value;
            }
            
            if( merge_data.empty() )
                break;
            
            auto it = merge_data.begin();
            
            if( std::filesystem::file_size( reducer_files[ reducers_idx ] ) )
                reducer_write[ reducers_idx ] << "\n";
            reducer_write[ reducers_idx ] << it->first << " " << it->second;
            reducer_write[ reducers_idx ].flush();
            merge_data.erase(it);
            
            reducers_idx = reducers_idx == reducers_count - 1 ? 0 : reducers_idx + 1;
        }
        
        reducers_idx = 0;
        for( const auto& data : merge_data )
        {
            if( std::filesystem::file_size( reducer_files[ reducers_idx ] ) )
                reducer_write[ reducers_idx ] << "\n";
            reducer_write[ reducers_idx ] << data.first << " " << data.second;
            reducer_write[ reducers_idx ].flush();
            reducers_idx = reducers_idx == reducers_count - 1 ? 0 : reducers_idx + 1;
        }
        
        for ( int i = 0; i < mappers_count; ++i )
        {
            mappers_read[i].close();
        }
        
        for ( int i = 0; i < reducers_count; ++i )
        {
            reducer_write[i].close();
        }
        
        // Создаём reducers_count потоков
        // В каждом потоке читаем свой файл (выход предыдущей фазы)
        // Применяем к строкам функцию reducer
        // Результат сохраняется в файловую систему 
        //             (во многих задачах выход редьюсера - большие данные, хотя в нашей задаче можно написать функцию reduce так, чтобы выход не был большим)
        
        auto apply_reduce = [ &reducer_files, this ]( int thread_num ) -> bool
        {
            if( !std::filesystem::file_size( reducer_files[ thread_num ] ) )
                return true;
            
            std::ifstream reduce_file( reducer_files[ thread_num ] );
            
            while( !reduce_file.eof() )
            {
                std::pair< std::string, int> data;
                
                reduce_file >> data.first >> data.second;
                
                if( !reducer( data ) )
                    return false;
            }
            
            return true;
        };
        
        std::vector< std::promise< bool > > accumulate_promise( reducers_count );
        std::vector< std::future< bool > > accumulate_future( reducers_count );

        std::vector< std::thread > reducer_threads;
        for ( std::size_t i = 0; i < reducers_count; ++i )
        {
            accumulate_future[ i ] = std::async( std::launch::async, apply_reduce, i );
        }

        bool found = true;
        for ( std::size_t i = 0; i < reducers_count; ++i )
        {
            if( !accumulate_future[ i ].get() )
                found = false;
        }
        
        for ( auto& th : reducer_threads )
        {
            if ( th.joinable() )
                th.join();
        }
        
        if( found )
        {
            std::ofstream output_file( output.string() );
            output_file << prefix_length;
        }
    }
    
private:
    struct Block
    {
        size_t from;
        size_t to;
    };
    
    std::string check_value( std::ifstream& is, size_t pos )
    {
        std::string buff( 1,'0' );
        is.seekg( pos, is.beg );
        is.read( &buff[0], 1 );
        
        return buff;
    }
    
    std::vector<Block> split_file( const std::filesystem::path& file, int blocks_count )
    {
        /**
         * Эта функция не читает весь файл.
         *
         * Определяем размер файла в байтах.
         * Делим размер на количество блоков - получаем границы блоков.
         * Читаем данные только вблизи границ.
         * Выравниваем границы блоков по границам строк.
         */
        
        std::vector< Block > blocks;
        
        if( !std::filesystem::exists( file ) )
            return blocks;
        
        std::uintmax_t whole_size = std::filesystem::file_size( file );
        int pos_interval = ceil( whole_size / ( blocks_count * 1.0 ) );
        
        size_t prev_from = 0;
        
        std::ifstream is( file.string(), std::ios::binary );
    
        for( int i = 0; i < blocks_count; ++i )
        {
            Block block;
            
            block.from = prev_from;

            if( i != blocks_count - 1 )
            {
                block.to = prev_from + pos_interval;
                
                while( check_value( is, block.to ) != "\n" )
                {
                    block.to++;
                }
                
                prev_from = block.to + 1;
            }
            else
            {
                block.to = whole_size;
            }
            
            blocks.push_back( block );
        }
        
        return blocks;
    }

    int mappers_count;
    int reducers_count;

    MapperFunction mapper;
    CombinerFunction combiner;
    ReducerFunction reducer;
};
