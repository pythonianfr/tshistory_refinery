module Cache exposing (main)

import Browser
import Html as H


type alias Model =
    { baseurl : String }


type Msg = Nothing


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    ( model, Cmd.none )


view : Model -> H.Html Msg
view model =
    H.p [ ] [ H.text "Hello" ]


sub model = Sub.none


type alias Input =
    { baseurl : String }

main : Program Input Model Msg
main =
    let
        init input =
            (Model input.baseurl, Cmd.none)
    in
        Browser.element
            { init = init
            , view = view
            , update = update
            , subscriptions = sub
            }
