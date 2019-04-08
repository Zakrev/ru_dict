# работа с словарями в sql-lite
import sqlite3

class sqll_dict_tag:
    _name:str = None
    _value:str = None

    def __init__(self, name:str, value:str):
        if name == None or value == None:
            raise Exception("неправильные аргументы")

        if name == "тип" or name == "id" or name == "слово":
            raise Exception("запрещенный параметр: {}".format(name))

        self._name = name
        self._value = value

class sqll_dict:
    __db_path = None
    _tag_none = "None"

    __now_table = None
    __now_word = None
    __now_word_id = None

    # если не удалось выбрать строку в таблице, то создается новая
    create_empty = False

    def __init__(self, db_path):
        self.__db_path = db_path

    def __prepare_word(self, word):
        if len(word) < 1:
            raise Exception("отсутствует слово")

        word = word.lower()

        for i in range(len(word)):
            if not word[i] in set("бвгджзйклмнпрстфхцчшщьъаеёиоуыэюя -"):
                raise Exception("{}: запрещенный символ: {}".format(word, word[i]))

        with sqlite3.connect(self.__db_path) as conn:
            curs = conn.cursor()
            curs.execute("SELECT id "
                         "FROM words "
                         "WHERE слово = '{}';".format(word))
            result = curs.fetchone()

            if result == None:
                curs.execute("INSERT "
                             "INTO words (слово) "
                             "VALUES ('{}');".format(word))
                #curs.execute("SELECT count(*) "
                #             "FROM words;")
                #result = curs.fetchone()
                #print("word: добавлено ({}): {}".format(result[0], word))
                curs.execute("SELECT id "
                             "FROM words "
                             "WHERE слово = '{}';".format(word))
                self.__now_word_id = int(curs.fetchone()[0])
                self.__now_word = word

                return

            self.__now_word_id = int(result[0])
            self.__now_word = word

    def __resolve_table(self, type_id):
        with sqlite3.connect(self.__db_path) as conn:
            curs = conn.cursor()
            curs.execute("SELECT таблица "
                         "FROM common_types "
                         "WHERE id = {};".format(type_id))
            result = curs.fetchone()

            if result == None:
                raise Exception("таблица {} не найдена".format(type_id))

            self.__now_table = result[0]

    def __resolve_row_id(self, tags):
        if (self.__now_table == None or self.__now_word == None
            or self.__now_word_id == None):
                raise Exception("не выбрано слово или таблица")

        with sqlite3.connect(self.__db_path) as conn:
            curs = conn.cursor()
            curs.execute("SELECT id "
                         "FROM {} "
                         "WHERE слово = {};".format(self.__now_table,
                                                    self.__now_word_id))
            results = curs.fetchall()

            if len(results) == 0:
                curs.execute("INSERT "
                             "INTO {} (слово) "
                             "VALUES ({});".format(self.__now_table,
                                                   self.__now_word_id))
                #curs.execute("SELECT count(*) "
                #             "FROM {};".format(self.__now_table))
                #result = curs.fetchone()
                #print("{}: добавлено ({}): {}".format(self.__now_table,
                #                                      result[0],
                #                                      self.__now_word))
                curs.execute("SELECT id "
                             "FROM {} "
                             "WHERE слово = {};".format(self.__now_table,
                                                        self.__now_word_id))

                return int(curs.fetchone()[0])

            line_id = None
            line_id_weight = -1
            unresolved_id_weight = None

            for result in results:
                id = int(result[0])
                id_weight = 0

                for tag in tags:
                    curs.execute('SELECT "{}" '
                                 "FROM {} "
                                 "WHERE id = {};".format(tag._name,
                                                         self.__now_table,
                                                         id))
                    tag_value = str(curs.fetchone()[0])
                    id_weight += 1

                    if tag_value != sqll_dict._tag_none and tag_value != str(tag._value):
                            id_weight = -2

                            break

                if id_weight == line_id_weight:
                    curs.execute("SELECT * "
                                 "FROM {} "
                                 "WHERE id = {};".format(self.__now_table,
                                                         id))
                    id_all = curs.fetchall()
                    curs.execute("SELECT * "
                                 "FROM {} "
                                 "WHERE id = {};".format(self.__now_table,
                                                         line_id))
                    line_id_all = curs.fetchall()

                    for id_val, line_id_val in zip(id_all[0:-1], line_id_all[0:-1]):
                        if str(id_val[0]) != str(line_id_val[0]):
                            unresolved_id_weight = id_weight
                            print("{}: {}: нельзя выбрать между"
                                  " {} и {}".format(self.__now_table,
                                                    self.__now_word,
                                                    line_id,
                                                    id))
                            break

                if id_weight > line_id_weight:
                    line_id = id
                    line_id_weight = id_weight

            if unresolved_id_weight != None and unresolved_id_weight == line_id_weight:
                line_id = None

            if line_id == None:
                if not self.create_empty:
                    raise Exception("{}: {}: не удалось выбрать "
                                    "из группы слов".format(self.__now_table,
                                                            self.__now_word))

                curs.execute("INSERT "
                             "INTO {} (слово) "
                             "VALUES ({});".format(self.__now_table,
                                                   self.__now_word_id))
                #curs.execute("SELECT count(*) "
                #             "FROM {};".format(self.__now_table))
                #result = curs.fetchone()
                #print("{}: добавлено ({}): {}".format(self.__now_table,
                #                                      result[0],
                #                                      self.__now_word))
                curs.execute("SELECT id "
                             "FROM {} "
                             "WHERE слово = {};".format(self.__now_table,
                                                        self.__now_word_id))
                return int(curs.fetchall()[-1][0])

            return line_id

    def free_word(self):
        self.__now_table = None
        self.__now_word = None
        self.__now_word_id = None

    def select_word(self, word, type_id):
        self.__resolve_table(type_id)
        self.__prepare_word(word)

    def set_tags(self, tags):
        row_id = self.__resolve_row_id(tags)

        with sqlite3.connect(self.__db_path) as conn:
            curs = conn.cursor()
            set_str = ""

            for tag in tags:
                set_str = set_str + '"{}" = {},'.format(tag._name, tag._value)

            curs.execute("UPDATE {} "
                         "SET {} "
                         "WHERE id = {};".format(self.__now_table,
                                                 set_str[0:-1],
                                                 row_id))

#if __name__ == "__main__":
#    dict = sqll_dict("words.db")
#    dict.create_empty = True
#    dict.select_word("авто", 2)
#    dict.set_tags([sqll_dict_tag("разряд существительного", 3),
#                   sqll_dict_tag("значение существительного", 2),
#                   sqll_dict_tag("склонение", 1),
#                   sqll_dict_tag("одушевленность", 2)])
