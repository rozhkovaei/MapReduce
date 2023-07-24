#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

class MergeSort
{
public:
  
    static void distribute( const std::string& filename, int thread_num, int& s )
    {
        std::ifstream fA( filename );
        
        std::stringstream filename_b;
        filename_b << "B" << thread_num << ".txt";
        
        std::stringstream filename_c;
        filename_c << "C" << thread_num << ".txt";
        
        std::ofstream f1( filename_b.str() );
        std::ofstream f2( filename_c.str() );
        s = 0; // s-четное, пишем на ленту 1, а при нечетном - на ленту 2
        
        std::pair< std::string, int> x, y;
        
        fA >> x.first >> x.second;
        f1 << x.first << " " << x.second;
        
        while ( !fA.eof() )
        {
            fA >> y.first >> y.second;
            if ( y.first < x.first )
                s++; // конец серии-переходим на другую ленту

            if ( s % 2 == 0 ) // при четном s запись на ленту 1 (f1)
            {
                f1 << "\n" << y.first << " " << y.second;
                f1.flush();
            }
            else // при нечетном s запись на ленту 2 (f2)
            {
                if( std::filesystem::file_size( filename_c.str() ) )
                    f2 << "\n";
                f2 << y.first << " " << y.second;
                f2.flush();
            }
            x = y;
        }
        fA.close();
        f1.close();
        f2.close();
    }

    static void merge( const std::string& filename, int thread_num )
    {
        std::ofstream fA( filename );
        
        std::stringstream filename_b;
        filename_b << "B" << thread_num << ".txt";
        
        std::stringstream filename_c;
        filename_c << "C" << thread_num << ".txt";
        
        std::ifstream f1( filename_b.str() );
        std::ifstream f2( filename_c.str() );
        
        std::pair< std::string, int > x, y;
        
    // Слияние:

        bool endf1; // Признак: endf1=1: f1 прочитан, все элементы записаны в fout
        bool endf2; // Аналогично для f2
        
        if( std::filesystem::file_size( filename_b.str() ) && !f1.eof() )
        {
            f1 >> x.first >> x.second;
            endf1 = 0;
        }
        else
            endf1 = 1;
        
        if( std::filesystem::file_size( filename_c.str() ) && !f2.eof() )
        {
            f2 >> y.first >> y.second;
            endf2 = 0;
        }
        else
            endf2 = 1;
        
        // Слияние:
        while ( !endf1 && !endf2 ) // пока оба файла не пусты
        {
            if( x.first <= y.first )
                save_fout( fA, filename, f1, x, endf1 );
            else
                save_fout( fA, filename, f2, y, endf2 );
        }
        // Теперь остатки одного из файлов:
        while( !endf1 )
            save_fout( fA, filename, f1, x, endf1 );
        while( !endf2 )
            save_fout( fA, filename, f2, y, endf2 );
        
        fA.close();
        f1.close();
        f2.close();
    }

    static void aggregate( const std::string& filename, int thread_num )
    {
        std::ifstream fA( filename );
        
        std::stringstream filename_b;
        filename_b << "D" << thread_num << ".txt";
        
        std::ofstream f1( filename_b.str() );
        
        std::pair< std::string, int> x, prev;
        
        int idx = 0;
        while ( !fA.eof() )
        {
            fA >> x.first >> x.second;
            
            if( idx )
            {
                if( prev.first == x.first )
                    prev.second += x.second;
                else
                {
                    if( std::filesystem::file_size( filename_b.str() ) )
                        f1 << "\n";
                    f1 << prev.first << " " << prev.second;
                    f1.flush();
                    prev = x;
                }
            }
            else
                prev = x;
            idx++;
        }
        
        if( std::filesystem::file_size( filename_b.str() ) )
            f1 << "\n";
        f1 << prev.first << " " << prev.second;
        f1.flush();
        
        std::filesystem::remove( filename );
        std::filesystem::rename( filename_b.str(), filename );
    }
    
private:
    
    static void save_fout( std::ofstream &f_out, const std::string& filename, std::ifstream &f_in, std::pair< std::string, int > &x, bool &endf )
    {
        if( !f_in.eof() )
        {
            if( std::filesystem::file_size( filename ) )
                f_out << "\n";
            f_out << x.first << " " << x.second; // записываем
            f_out.flush();

            f_in >> x.first >> x.second; // и читаем
        }
        else
        {
            if( std::filesystem::file_size( filename ) )
                f_out << "\n";
            f_out << x.first << " " << x.second; // только записываем
            f_out.flush();
            
            endf = 1;
        }
    }
};

