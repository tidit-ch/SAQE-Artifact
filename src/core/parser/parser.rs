use super::ast::{CropASTNode, CropBinaryExpr, CropExpr, CropFunctionCall, CropOperator};
use crate::utils::error::{SaqeError, SaqeResult};
use anyhow::Context;
use datafusion::sql::{
    parser::{DFParser, DFParserBuilder},
    sqlparser::{keywords::Keyword, tokenizer, tokenizer::Token},
};

pub struct CustomSqlParser<'a> {
    sql: &'a str,
    df_parser: DFParser<'a>,
}

impl<'a> CustomSqlParser<'a> {
    pub fn new(sql: &'a str) -> SaqeResult<Self> {
        Ok(Self {
            sql,
            df_parser: DFParserBuilder::new(sql).build()?,
        })
    }

    fn assert_token(&mut self, expected: Token) -> SaqeResult<()> {
        let token = self.df_parser.parser.next_token();
        if token.token != expected {
            return Err(SaqeError::ParseError {
                expected,
                found: token,
            });
        }
        Ok(())
    }

    pub fn move_head_to_crop(&mut self) {
        let mut token = self.df_parser.parser.peek_token();
        while token != Token::EOF {
            match token.token {
                Token::Word(w) if w.value.to_lowercase() == "crop" => {
                    return;
                }
                _ => {
                    self.df_parser.parser.next_token();
                    token = self.df_parser.parser.peek_token();
                }
            }
        }
    }

    pub fn print_all_tokens(&mut self) {
        let mut i = 0;
        let mut token = self.df_parser.parser.peek_nth_token(i);
        while token.token != Token::EOF {
            println!("Token {}: {:?}", i, token);
            i += 1;
            token = self.df_parser.parser.peek_nth_token(i);
        }
    }

    //
    /**
    * The main entry point for parsing the custom SQL with crop function.
    * Crop function = crop(column_name, expression)
    * Ex: SELECT crop(trajectory, follows('1372637573')) FROM my_table;
        SELECT crop(
            trajectory,
            during('1372637573', '1372638573') AND not_during('1372637873', '1372637973')
        ) FROM my_table;
    */
    pub fn parse(&mut self) -> SaqeResult<(String, Option<CropASTNode>)> {
        if !self.contains_crop() {
            return Ok((self.sql.to_string(), None));
        }
        self.move_head_to_crop();
        let start_span = self.df_parser.parser.peek_token().span;
        self.df_parser.parser.advance_token(); // consume the "crop" token
        let mut crop_project_udf = CropASTNode::new();

        self.assert_token(Token::LParen)
            .context("Failed to parse opening parenthesis in crop function")?;

        let column_name = self.parse_column_name()?;
        crop_project_udf.set_column_name(column_name.clone());

        // consume the comma
        self.assert_token(Token::Comma)
            .context("Failed to parse comma in crop function")?;

        let expr = self.parse_expression()?;
        crop_project_udf.set_expr(expr);

        self.assert_token(Token::RParen)
            .context("Failed to parse closing parenthesis in crop function")?;

        // Check for alias
        if let Some(alias) = self.parse_alias()? {
            crop_project_udf.set_alias(alias);
        }

        let end_span = self.df_parser.parser.peek_token().span;
        let modified_sql = self.get_new_sql(start_span, end_span);
        Ok((modified_sql, Some(crop_project_udf)))
    }

    fn parse_column_name(&mut self) -> SaqeResult<String> {
        let mut token = self.df_parser.parser.next_token();
        let mut next_token = self.df_parser.parser.peek_token();
        let mut result = String::new();
        while next_token.token == Token::Period {
            if let Token::Word(tokenizer::Word { value, .. }) = token.token {
                result.push_str(&value.as_str());
                result.push_str(".");
            } else {
                return Err(SaqeError::ParseError {
                    expected: Token::Word(tokenizer::Word {
                        value: "column_name".to_string(),
                        quote_style: None,
                        keyword: Keyword::NoKeyword,
                    }),
                    found: next_token,
                })
                .context("Failed to parse column name in crop function")?;
            }
            self.df_parser.parser.advance_token(); // consume the period token
            token = self.df_parser.parser.next_token();
            next_token = self.df_parser.parser.peek_token();
        }
        // The last token should be the column name
        if let Token::Word(tokenizer::Word { value, .. }) = token.token {
            result.push_str(&value.as_str());
        } else {
            return Err(SaqeError::ParseError {
                expected: Token::Word(tokenizer::Word {
                    value: "column_name".to_string(),
                    quote_style: None,
                    keyword: Keyword::NoKeyword,
                }),
                found: token,
            })
            .context("Failed to parse column name in crop function")?;
        }
        Ok(result)
    }

    fn get_prev_non_whitespace_index(&self, index: usize) -> usize {
        let mut i = index;
        let sql = self.sql.chars().collect::<Vec<char>>();
        while i > 0 && sql[i].is_whitespace() {
            i -= 1;
        }
        i
    }

    fn get_next_non_whitespace_index(&self, index: usize) -> usize {
        let mut i = index;
        let sql = self.sql.chars().collect::<Vec<char>>();
        while i < sql.len() && sql[i].is_whitespace() {
            i += 1;
        }
        i
    }

    fn get_new_sql(&self, start_span: tokenizer::Span, end_span: tokenizer::Span) -> String {
        let mut start_index = self.get_string_index(start_span);
        let mut end_index = self.get_string_index(end_span);
        let sql_char = self.sql.chars().collect::<Vec<char>>();
        start_index = self.get_prev_non_whitespace_index(start_index - 1);
        end_index = self.get_next_non_whitespace_index(end_index);
        let is_start_comma = start_index > 0 && sql_char[start_index] == ',';
        let is_end_comma = end_index < sql_char.len() && sql_char[end_index] == ',';
        (start_index, end_index) = if is_start_comma && is_end_comma {
            (start_index - 1, end_index)
        } else if is_start_comma {
            (start_index - 1, end_index)
        } else if is_end_comma {
            (start_index, end_index + 1)
        } else {
            (start_index, end_index)
        };
        self.sql[0..start_index + 1].to_string()
            + " "
            + self.sql[(end_index - 1)..].to_string().as_str()
    }

    fn get_string_index(&self, span: tokenizer::Span) -> usize {
        let mut index = 0;
        let mut line = 1;
        let mut column = 1;
        for (i, c) in self.sql.char_indices() {
            if line == span.start.line && column == span.start.column {
                index = i;
                break;
            }
            if c == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }
        index
    }

    //  expression = (function_call) | (expression) AND (expression) | (expression) OR (expression)
    pub fn parse_expression(&mut self) -> SaqeResult<CropExpr> {
        let token = self.df_parser.parser.peek_token();
        let mut left: CropExpr;
        if token.token == Token::LParen {
            self.df_parser.parser.advance_token();
            left = self
                .parse_expression()
                .context("Failed to parse expression")?;
            self.assert_token(Token::RParen)
                .context("Failed to parse closing parenthesis in expression of crop function")?;
        } else {
            left = self.parse_function_call()?;
        }
        while let Some(keyword) = self
            .df_parser
            .parser
            .parse_one_of_keywords(&[Keyword::AND, Keyword::OR])
        {
            let operator = match keyword {
                Keyword::AND => CropOperator::And,
                Keyword::OR => CropOperator::Or,
                _ => unreachable!(),
            };
            let token = self.df_parser.parser.peek_token();
            let right = if token.token == Token::LParen {
                self.df_parser.parser.advance_token();
                let right = self
                    .parse_expression()
                    .context("Failed to parse expression")?;
                self.assert_token(Token::RParen).context(
                    "Failed to parse closing parenthesis in expression of crop function",
                )?;
                right
            } else {
                self.parse_function_call()?
            };
            left = CropExpr::BinaryExpr(CropBinaryExpr {
                left: Box::new(left),
                operator,
                right: Box::new(right),
            });
        }
        Ok(left)
    }

    // function call = function_name(arg1, arg2, ...)
    // NOTE: currently we are only supporting string arguments.
    pub fn parse_function_call(&mut self) -> SaqeResult<CropExpr> {
        let token = self.df_parser.parser.next_token();
        let function_name = if let Token::Word(tokenizer::Word { value, .. }) = token.token {
            value
        } else {
            return Err(SaqeError::ParseError {
                expected: Token::Word(tokenizer::Word {
                    value: "function_name".to_string(),
                    quote_style: None,
                    keyword: Keyword::NoKeyword,
                }),
                found: token,
            })
            .context("Failed to parse function name in crop function")?;
        };
        self.assert_token(Token::LParen)
            .context("Failed to parse opening parenthesis in arg function call of crop function")?;
        let mut arguments: Vec<String> = Vec::new();
        let mut token = self.df_parser.parser.next_token();
        while token.token != Token::RParen {
            let arg = if let Token::SingleQuotedString(value) = token.token {
                value
            } else {
                return Err(SaqeError::ParseError {
                    expected: Token::SingleQuotedString("argument".to_string()),
                    found: token,
                })
                .context("Failed to parse argument in function call of crop function")?;
            };
            arguments.push(arg);
            token = self.df_parser.parser.next_token();
            if token.token == Token::Comma {
                token = self.df_parser.parser.next_token();
            } else if token.token != Token::RParen {
                return Err(SaqeError::ParseError {
                    expected: Token::RParen,
                    found: token,
                })
                .context(
                    "Failed to parse closing parenthesis in arg function call of crop function",
                )?;
            }
        }
        Ok(CropExpr::FunctionCall(CropFunctionCall::new(
            function_name,
            arguments,
        )))
    }

    fn parse_alias(&mut self) -> SaqeResult<Option<String>> {
        let next_token = self.df_parser.parser.peek_token();
        if let Token::Word(tokenizer::Word { keyword, .. }) = next_token.token {
            if keyword == Keyword::AS {
                self.df_parser.parser.advance_token(); // consume AS
                let alias_token = self.df_parser.parser.next_token();
                if let Token::Word(tokenizer::Word { value: alias, .. }) = alias_token.token {
                    return Ok(Some(alias));
                } else {
                    return Err(SaqeError::ParseError {
                        expected: Token::Word(tokenizer::Word {
                            value: "alias".to_string(),
                            quote_style: None,
                            keyword: Keyword::NoKeyword,
                        }),
                        found: alias_token,
                    })
                    .context("Failed to parse alias for crop function output")?;
                }
            }
        }
        Ok(None)
    }

    // TODO: CRITICAL: this approach was only for testing. It is not robust enough for production use. It will fail if the SQL contains a comment with the word "crop" in it, or if the word "crop" appears in a string literal, or if the word "crop" appears in a column name or table name. We need to implement a proper SQL parser that can handle these cases.
    pub fn contains_crop(&self) -> bool {
        self.sql.contains("crop")
    }
}
