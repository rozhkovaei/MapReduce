#include "mapreduce.h"
#include "merge_sort.h"

/**
 * В этом файле находится клиентский код, который использует наш MapReduce фреймворк.
 * Этот код знает о том, какую задачу мы решаем.
 * Задача этого кода - верно написать мапер, редьюсер, запустить mapreduce задачу, обработать результат.
 * Задача - найти минимальную длину префикса, который позволяет однозначно идентифицировать строку в файле.
 * Задача не решается в одну mapreduce задачу. Нужно делать несколько запусков.
 * 
 * Как предлагаю делать я:
 * Выделяем первые буквы слов (в мапере), решаем для них задачу "определить, есть ли в них повторы".
 * Если не прокатило, повторяем процедуру, выделяя первые две буквы.
 * И т.д. В итоге найдём длину префикса, который однозначно определяет строку.
 * 
 * Здесь описано то, как я примерно решал бы задачу, это не руководство к действию, а просто пояснение к основному тексту задания.
 * Вы можете поступать по-своему (не как я описываю), задание творческое!
 * Можете делать так, как написано, если считаете, что это хорошо.
 */

static constexpr int MAX_PREFIX_LENGTH = 256;

int main( int argc, char* argv[] )
{
    std::filesystem::path input( argv[ 1 ] );
    std::filesystem::path output( "out.txt"  );
    int mappers_count = std::atoi( argv[ 2 ] );
    int reducers_count = std::atoi( argv[ 3 ] );

    MapReduce mr( mappers_count, reducers_count );
    
    std::filesystem::remove( output );
    
    for( int prefix_length = 1; prefix_length < MAX_PREFIX_LENGTH; ++prefix_length )
    {
        mr.set_mapper(
            [ prefix_length ]( const std::string& line ) -> std::pair< std::string, int >
            {
                //     * получает строку,
                //     * выделяет префикс,
                //     * возвращает пары (префикс, 1).
                
                std::string prefix( line, 0, prefix_length );
                return std::make_pair( prefix, 1 );
            }
        );
        
        mr.set_combiner(
            []( const std::string& filename, int thread_num ) -> void
            {
                //     * сортирует файл от маппера, использую внешнюю сортировку

                int s = 2;
                while ( s >= 1 ) /* закончим, когда переходов не будет */
                {
                 // распределение A по лентам (файлам) B и C:
                    MergeSort::distribute( filename, thread_num, s );
                 // Слияние B и C в A:
                    if ( s >= 1 )
                        MergeSort::merge( filename, thread_num );
                }
                
                //     * на втором проходе выполняет предаггрегирование
                MergeSort::aggregate( filename, thread_num );
            }
        );
        
        mr.set_reducer(
            []( std::pair< std::string, int >& data ) -> bool
            {
                // моё предложение:
                //     * получает пару (префикс, число),
                //     * если текущий префикс совпадает с предыдущим или имеет число > 1, то возвращает false,
                //     * иначе возвращает true.
                //
                // Почему тут написано "число", а не "1"?
                // Чтобы учесть возможность добавления фазы combine на выходе мапера.
                // Почитайте, что такое фаза combine в hadoop.
                // Попробуйте это реализовать, если останется время.
                
                return data.second > 1 ? false : true;
            }
        );
        
        mr.run( input, output, prefix_length );
        
        if( std::filesystem::exists( output ) ) // файл не пустой, результат есть
            return 0;
    }

    return 0;
}
