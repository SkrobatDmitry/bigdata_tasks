# Local
---
The script is used for searching movies in the CSV-format file by the following parameters: genre, release year, title matches. It also combines [mapper.py](mapper.py) and [reducer.py](reducer.py) under one interface for local emulation of MapReduce work.

## Usage
---
```text
usage: get-movies.sh [-h] [--N amount] [--genres genres] [--year-from year] [--year-to year] [--regexp regexp]
```

### Parameters
---
- `-h, --help` - show help message and exit.
- `--N amount` - number of movies of each genre to output.
- `--genres genres` - requested movies genres.
- `--year-from year` - the first filter by the year the movie was made.
- `--year-to year` - the second filter by the year the movie was made.
- `--regxep regexp` - filter of movies title or their parts.

### Help message
---
Use `-h` or `--help` to get the help message. For example:
```bash
bash local/get-movies.sh --help
```

The script produces the following output:
```text
usage: get-movies.sh [-h] [--N amount] [--genres genres] [--year-from year] [--year-to year] [--regexp regexp]

optional arguments:
  -h, --help        show this help message and exit

  --N amount        number of movies of each rating to output
  --genres genres   requested movies genres
  --year-from year  the first filter by the year the movie was made
  --year-to year    the second filter by the year the movie was made
  --regexp regexp   filter of movies title or their parts
```

### Number
---
Use the `--N` argument to specify how many movies for each genre to show. For example:
```bash
bash local/get-movies.sh --N 1
```

In this case, the script displays one movie for each genre:
```text
Action,Ant-Man and the Wasp,2018
Adventure,A Wrinkle in Time,2018
Animation,Bungo Stray Dogs: Dead Apple,2018
Children,A Wrinkle in Time,2018
Comedy,Ant-Man and the Wasp,2018
Crime,BlacKkKlansman,2018
Documentary,Spiral,2018
Drama,A Quiet Place,2018
Fantasy,A Wrinkle in Time,2018
Film-Noir,Bullet to the Head,2012
Horror,A Quiet Place,2018
IMAX,Star Wars: Episode VII - The Force Awakens,2015
Musical,Strange Magic,2015
Mystery,Annihilation,2018
Romance,Mamma Mia: Here We Go Again!,2018
Sci-Fi,A Wrinkle in Time,2018
Thriller,A Quiet Place,2018
War,Darkest Hour,2017
Western,The Beguiled,2017
```

### Genres
---
Use the `--genres` argument to specify which genres movies should be displayed in. For example:
```bash
bash local/get-movies.sh --N 2 --genres "Crime|IMAX|Thriller"
```

In this case, the script displays two movies for each requested genre:
```text
Crime,BlacKkKlansman,2018
Crime,Death Wish,2018
IMAX,Star Wars: Episode VII - The Force Awakens,2015
IMAX,300: Rise of an Empire,2014
Thriller,A Quiet Place,2018
Thriller,Alpha,2018
```

### Year
---
Specify `--year-from` or `--year-to` or both arguments to determine from which to which year the movies will be displayed. For example:
```bash
bash local/get-movies.sh --N 3 --genres "Action|Animation" --year-from 1920 --year-to 1930
```

In this case, the script displays three movies released between 1920 and 1930 for each requested genre:
```text
Action,All Quiet on the Western Front,1930
Action,Aelita: The Queen of Mars (Aelita),1924
Action,"Thief of Bagdad, The",1924
Animation,Steamboat Willie,1928
```

### Regexp
---
Use the `--regexp` argument to determine if a title matches a given regular expression. For example:
```bash
bash local/get-movies.sh --N 1 --regexp "Panda"
```

In this case, the script displays one movie with Panda in the title for each genre:
```text
Action,Kung Fu Panda 3,2016
Adventure,Kung Fu Panda 3,2016
Animation,Kung Fu Panda 3,2016
Children,Kung Fu Panda 2,2011
Comedy,Kung Fu Panda 2,2011
IMAX,Kung Fu Panda 2,2011
```