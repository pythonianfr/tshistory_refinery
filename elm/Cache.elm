module Cache exposing (main)

import Browser
import Html as H
import Json.Decode as D


type alias Policy =
    { name : String
    , ready : Bool
    , initial_revdate : String
    , from_date : String
    , look_before : String
    , look_after : String
    , revdate_rule : String
    , schedule_rule : String
    }


type alias Model =
    { baseurl : String
    , policies : List Policy
    }


policydecoder =
    D.map8 Policy
        (D.field "name" D.string)
        (D.field "ready" D.bool)
        (D.field "initial_revdate" D.string)
        (D.field "from_date" D.string)
        (D.field "look_before" D.string)
        (D.field "look_after" D.string)
        (D.field "revdate_rule" D.string)
        (D.field "schedule_rule" D.string)


policiesdecoder =
    D.list policydecoder


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
            (Model input.baseurl [], Cmd.none)
    in
        Browser.element
            { init = init
            , view = view
            , update = update
            , subscriptions = sub
            }
